
#ifndef BPLUS_C
#define BPLUS_C

#include "MyDB_INRecord.h"
#include "MyDB_BPlusTreeReaderWriter.h"
#include "MyDB_PageReaderWriter.h"
#include "MyDB_PageListIteratorSelfSortingAlt.h"
#include "RecordComparator.h"
#include <climits>
#include <algorithm>

MyDB_BPlusTreeReaderWriter :: MyDB_BPlusTreeReaderWriter (string orderOnAttName, MyDB_TablePtr forMe, 
	MyDB_BufferManagerPtr myBuffer) : MyDB_TableReaderWriter (forMe, myBuffer) {

	// find the ordering attribute
	auto res = forMe->getSchema ()->getAttByName (orderOnAttName);

	// remember information about the ordering attribute
	orderingAttType = res.second;
	whichAttIsOrdering = res.first;

	// and the root location
	rootLocation = getTable ()->getRootLocation ();
}

MyDB_RecordIteratorAltPtr MyDB_BPlusTreeReaderWriter :: getSortedRangeIteratorAlt (MyDB_AttValPtr low, MyDB_AttValPtr high) {  // REQUIRED
	/// discoverPages
	vector<MyDB_PageReaderWriter> pageList;
	// check for page count
	// if page count = 0, add a single anonymous page into list
	discoverPages(this->rootLocation, pageList, low, high);

	// Do first
	MyDB_RecordPtr lhs = getEmptyRecord();
	MyDB_RecordPtr rhs = getEmptyRecord();
	MyDB_RecordPtr myRec = getEmptyRecord();
    MyDB_INRecordPtr llow = getINRecord();
    llow->setKey(low);
    MyDB_INRecordPtr hhigh = getINRecord();
    hhigh->setKey(high);

    // // build the comparison functions
    function <bool ()> comparator = buildComparator(lhs, rhs);
    function <bool ()> lowComparator = buildComparator(myRec, llow);
    function <bool ()> highComparator = buildComparator(hhigh, myRec);

	return make_shared<MyDB_PageListIteratorSelfSortingAlt>(pageList, lhs, rhs, comparator, myRec, lowComparator, highComparator, true);
}

MyDB_RecordIteratorAltPtr MyDB_BPlusTreeReaderWriter :: getRangeIteratorAlt (MyDB_AttValPtr low, MyDB_AttValPtr high) {  // REQUIRED
	/// discoverPages
	vector<MyDB_PageReaderWriter> pageList;
	// check for page count
	// if page count = 0, add a single anonymous page into list
	if (getNumPages() == 0) {
		(*this)[0];  // ideally makes 1 page
	}
	discoverPages(this->rootLocation, pageList, low, high);

	// Do first
	MyDB_RecordPtr lhs = getEmptyRecord();
	MyDB_RecordPtr rhs = getEmptyRecord();
	MyDB_RecordPtr myRec = getEmptyRecord();

    MyDB_INRecordPtr llow = getINRecord();
    llow->setKey(low);
    llow->recordContentHasChanged();

    MyDB_INRecordPtr hhigh = getINRecord();
    hhigh->setKey(high);
    hhigh->recordContentHasChanged();

    // // build the comparison functions
    function <bool ()> comparator = buildComparator(lhs, rhs);
    function <bool ()> lowComparator = buildComparator(myRec, llow);
    function <bool ()> highComparator = buildComparator(hhigh, myRec);

	return make_shared<MyDB_PageListIteratorSelfSortingAlt>(pageList, lhs, rhs, comparator, myRec, lowComparator, highComparator, false);
}



bool MyDB_BPlusTreeReaderWriter::discoverPages(int whichPage, vector<MyDB_PageReaderWriter> &list, MyDB_AttValPtr low, MyDB_AttValPtr high) {

    MyDB_PageReaderWriter currentPage = (*this)[whichPage];

    // Base Case: Leaf Page
    if (currentPage.getType() == MyDB_PageType::RegularPage) {
        list.push_back(currentPage);
        return true;
    }

    // Recursive Step: Internal Page
    if (currentPage.getType() == MyDB_PageType::DirectoryPage) {

        MyDB_RecordIteratorAltPtr iter = currentPage.getIteratorAlt();
        MyDB_INRecordPtr currentRec = getINRecord();

        // Prepare low/high sentinel records for comparisons
        MyDB_INRecordPtr lowRec = getINRecord();
        lowRec->setKey(low);
        lowRec->recordContentHasChanged();

        MyDB_INRecordPtr highRec = getINRecord();
        highRec->setKey(high);
        highRec->recordContentHasChanged();

        // Initialize prevRec to -infinity (so first child range is (-inf, key0])
        MyDB_AttValPtr minAtt = orderingAttType->createAtt();
        minAtt->fromInt(INT_MIN);
        MyDB_INRecordPtr prevRec = getINRecord();
        prevRec->setKey(minAtt);
        prevRec->recordContentHasChanged();
        
        
        // This flag will track if we've hit the parent-of-leaves level.
        bool atParentOfLeaves = false;

        while (iter->advance()) {
            iter->getCurrent(currentRec);

            // currKey = child_max, prevKey = child_min
            // Test if child_max < low
            bool currLessLow = buildComparator(currentRec, lowRec)();   // currKey < low ?
            // Test if high < prevKey
            bool highLessPrev = buildComparator(highRec, prevRec)(); // high < prevKey ?

            if (!currLessLow && !highLessPrev) {

                // Check the flag. If we are at the parent-of-leaves level,
                // we no longer need to recurse. We can add the leaf directly.
                if (atParentOfLeaves) {
                    list.push_back((*this)[currentRec->getPtr()]);
                } else {
                    // Otherwise, we are in an upper level, so we must recurse.
                    bool childWasLeaf = discoverPages(currentRec->getPtr(), list, low, high);
                    
                    // If the child was a leaf, set the flag for all subsequent iterations of this loop.
                    if (childWasLeaf) {
                        atParentOfLeaves = true;
                    }
                }
            }
            

            prevRec->setKey(this->getKey(currentRec));
            prevRec->recordContentHasChanged();
        }
        return false;
    }
    
    return false;
}




void MyDB_BPlusTreeReaderWriter::append(MyDB_RecordPtr appendMe) {
    // Create empty BPlusTree
    //cerr << " NUM PAGES: " << getNumPages() << endl;
    if (getTable()->lastPage() <= 0) {
        rootLocation = 0;
        int leafLocation = 1;

        // Create the root and first leaf
        (*this)[rootLocation].setType(MyDB_PageType::DirectoryPage);
        (*this)[leafLocation].setType(MyDB_PageType::RegularPage);

        // Create the initial "infinity" record
        MyDB_INRecordPtr infRec = getINRecord();
        infRec->setPtr(leafLocation);
        infRec->recordContentHasChanged();

        // Append to the root page directly
        (*this)[rootLocation].append(infRec);

        // Persist table metadata
        getTable()->setRootLocation(rootLocation);
    }
    
    // Call the private, recursive append method, starting from the root.
    //cout << "append public rootLocation:  " << rootLocation << " and last page: " << getTable()->lastPage() << endl;

    MyDB_RecordPtr newSplitRecord = append(rootLocation, appendMe);

    // If the private append returns a non-null pointer, it means the root itself has split.
    if (newSplitRecord != nullptr) {
        
        // 1. Get a new page from the buffer manager to serve as the new root.
        int newRootIndex = getTable()->lastPage() + 1;

        MyDB_PageReaderWriter newRootPage = (*this)[newRootIndex];
        newRootPage.setType(MyDB_PageType::DirectoryPage);

        // Notify the record from the split.
        newSplitRecord->recordContentHasChanged();

        // 2. Add the record returned by the split, which points to the new page.
        newRootPage.append(newSplitRecord);

        // 3. Create a record that points to the OLD root page.
        // The key for this record is "infinity" by default, which is correct for
        // the rightmost pointer in a B+-Tree internal node.
        MyDB_INRecordPtr oldRootPtrRec = getINRecord();
        oldRootPtrRec->setPtr(rootLocation);

        // Notify this record that its pointer was just set.
        oldRootPtrRec->recordContentHasChanged();

        newRootPage.append(oldRootPtrRec);

        // 4. Update the tree's metadata to make the new page the official root.
        rootLocation = newRootIndex;
        getTable()->setRootLocation(rootLocation);
    }
}


MyDB_RecordPtr MyDB_BPlusTreeReaderWriter :: split (MyDB_PageReaderWriter splitMe, MyDB_RecordPtr andMe) {

    MyDB_PageType splitPageType = splitMe.getType();
    int newPageIdx = getTable()->lastPage() + 1;
    MyDB_PageReaderWriter newPage = (*this)[newPageIdx];
    newPage.setType(splitPageType);

    vector<MyDB_RecordPtr> records;
    MyDB_RecordIteratorAltPtr iter = splitMe.getIteratorAlt();

    // This part is already correct and clever.
    MyDB_RecordPtr currentRec = (splitPageType == MyDB_PageType::RegularPage) ? getEmptyRecord() : getINRecord();

    while(iter->advance()) {
        iter->getCurrent(currentRec);

        // Create a copy object of the CORRECT type.
        // If we are splitting a leaf, make a regular record.
        // If we are splitting an internal node, make an INRecord.
        MyDB_RecordPtr newCopy = (splitPageType == MyDB_PageType::RegularPage) ? getEmptyRecord() : getINRecord();

        // Now the binary copy is from a matching type to a matching type.
        void* tempBytes = malloc(currentRec->getBinarySize());
        currentRec->toBinary(tempBytes);
        newCopy->fromBinary(tempBytes);
        free(tempBytes);

        records.push_back(newCopy);
    }

    // Also copy 'andMe' into the correct type of record.
    MyDB_RecordPtr andMeCopy = (splitPageType == MyDB_PageType::RegularPage) ? getEmptyRecord() : getINRecord();
    void* tempBytes = malloc(andMe->getBinarySize());
    andMe->toBinary(tempBytes);
    andMeCopy->fromBinary(tempBytes);
    free(tempBytes);
    records.push_back(andMeCopy);

    // The rest of your split logic is correct.
    std::sort(records.begin(), records.end(),[&](const MyDB_RecordPtr& r1, const MyDB_RecordPtr& r2) {
        return buildComparator(r1, r2)();
    });

    splitMe.clear();
    splitMe.setType(splitPageType);

    int midpoint = records.size() / 2;
    
    if (splitPageType == MyDB_PageType::RegularPage) {
        for (int i = 0; i < midpoint; i++) {
            newPage.append(records[i]);
        }
        for (size_t i = midpoint; i < records.size(); i++) {
            splitMe.append(records[i]);
        }

        MyDB_INRecordPtr promotedRec = getINRecord();
        promotedRec->setKey(getKey(records[midpoint]));
        promotedRec->setPtr(newPageIdx);
        promotedRec->recordContentHasChanged();
        return promotedRec;
    } 
    else { // Internal Page
        for (int i = 0; i < midpoint; i++) {
            newPage.append(records[i]);
        }
        for (size_t i = midpoint + 1; i < records.size(); i++) {
            splitMe.append(records[i]);
        }

        MyDB_INRecordPtr middleRecord = static_pointer_cast<MyDB_INRecord>(records[midpoint]);
        middleRecord->setPtr(newPageIdx);
        middleRecord->recordContentHasChanged();
        return middleRecord;
    }
}



// appends a record to the named page; if there is a split, then an MyDB_INRecordPtr is returned that
// points to the record holding the (key, ptr) pair pointing to the new page.  Note that the new page
// always holds the lower 1/2 of the records on the page; the upper 1/2 remains in the original page
MyDB_RecordPtr MyDB_BPlusTreeReaderWriter::append(int whichPage, MyDB_RecordPtr appendMe) {
    
    //cerr << "\n=== [append] Enter page " << whichPage
        //<< " type=" << ((*this)[whichPage].getType() == MyDB_PageType::DirectoryPage ? "DIR" : "LEAF")
        //<< " ===" << endl;

    MyDB_PageReaderWriter currPage = (*this)[whichPage];
    
    // Case 1: The page is an internal (directory) node.
    if (currPage.getType() == MyDB_PageType::DirectoryPage) {
        
        MyDB_RecordIteratorAltPtr iterator = currPage.getIteratorAlt();
        int childPageIdx = -1;

        // Create a valid, non-null record object before the loop.
        // This object will be reused by the iterator to hold data.
        MyDB_INRecordPtr currINRecord = getINRecord();

        while (iterator->advance()) {
            // Now, getCurrent is given a valid object to fill.
            iterator->getCurrent(currINRecord);

            if (buildComparator(appendMe, currINRecord)()) {
                childPageIdx = currINRecord->getPtr();
                break;
            }
            childPageIdx = currINRecord->getPtr();
        }

        if (childPageIdx == -1) {
            // This case should not be reached in a valid tree.
            return nullptr;
        }

        // Recursively call append on the correct child.
        //cerr << "[append] Page " << whichPage << " type=" 
        //<< (currPage.getType() == MyDB_PageType::RegularPage ? "LEAF" : "DIR") 
        //<< endl;

        MyDB_RecordPtr newSplitRecord = append(childPageIdx, appendMe);

        // If the child split, we need to handle the new record.
        if (newSplitRecord != nullptr) {
            if (currPage.append(newSplitRecord)) {
                // 1. The record fit. Create the buffer records that sortInPlace will use.
                MyDB_INRecordPtr lhs = getINRecord();
                MyDB_INRecordPtr rhs = getINRecord();

                // 2. Build the comparator FROM these specific buffer records.
                function<bool()> comparator = buildComparator(lhs, rhs);

                // 3. Call sortInPlace with the correct comparator and buffers.
                currPage.sortInPlace(comparator, lhs, rhs);

                return nullptr;
            } else {
                // This page is also full and must split.
                return split(currPage, newSplitRecord);
            }
        } else {
            // Child did not split, so no more work to do.
            return nullptr;
        }

    // Case 2: The page is a leaf node.
    } else {
        if (currPage.append(appendMe)) {
            // The record fit. We are done.
            return nullptr;
        } else {
            // The page is full and must split.
            return split(currPage, appendMe);
        }
    }
}


MyDB_INRecordPtr MyDB_BPlusTreeReaderWriter :: getINRecord () {
	return make_shared <MyDB_INRecord> (orderingAttType->createAttMax ());
}

void MyDB_BPlusTreeReaderWriter :: printTree () {  // REQUIRED - need to make test case for this too
	queue<MyDB_PageReaderWriter> nodeQueue;  // Make queue
	nodeQueue.push((*this)[this->rootLocation]);  // Push root node
	MyDB_INRecordPtr recordPtr = getINRecord();

	// While there are still pages...
	while(!nodeQueue.empty()) {
		int levelSize = nodeQueue.size();

		for (int i = 0; i < levelSize; i++) {
			// Get front element + pop it
			MyDB_PageReaderWriter temp = nodeQueue.front();
			nodeQueue.pop();
			// Get iterator
			MyDB_RecordIteratorAltPtr iterator = temp.getIteratorAlt();
			// Begin printing
			cout << "[";
			bool hasNext = true;
			while (hasNext) {  // While this node has a next...
				iterator->getCurrent(recordPtr);  // Get current pointer data
				if (temp.getType() == MyDB_PageType::DirectoryPage) {  // If internal page, push next page onto list
					nodeQueue.push((*this)[recordPtr->getPtr()]);
				}
				cout << MyDB_RecordPtr(recordPtr);  // Print data
				if (!iterator->advance()) {  // Advances pointer. If not...
					cout << "] ";  // Prints end brackets
                    hasNext = false;
				}
			}
		}
		cout << endl;
	}
}




MyDB_AttValPtr MyDB_BPlusTreeReaderWriter :: getKey (MyDB_RecordPtr fromMe) {

	// in this case, got an IN record
	if (fromMe->getSchema () == nullptr) 
		return fromMe->getAtt (0)->getCopy ();

	// in this case, got a data record
	else 
		return fromMe->getAtt (whichAttIsOrdering)->getCopy ();
}

function <bool ()>  MyDB_BPlusTreeReaderWriter :: buildComparator (MyDB_RecordPtr lhs, MyDB_RecordPtr rhs) {

	MyDB_AttValPtr lhAtt, rhAtt;

	// in this case, the LHS is an IN record
	if (lhs->getSchema () == nullptr) {
		lhAtt = lhs->getAtt (0);	

	// here, it is a regular data record
	} else {
		lhAtt = lhs->getAtt (whichAttIsOrdering);
	}

	// in this case, the LHS is an IN record
	if (rhs->getSchema () == nullptr) {
		rhAtt = rhs->getAtt (0);	

	// here, it is a regular data record
	} else {
		rhAtt = rhs->getAtt (whichAttIsOrdering);
	}
	
	// now, build the comparison lambda and return
	if (orderingAttType->promotableToInt ()) {
		return [lhAtt, rhAtt] {return lhAtt->toInt () < rhAtt->toInt ();};
	} else if (orderingAttType->promotableToDouble ()) {
		return [lhAtt, rhAtt] {return lhAtt->toDouble () < rhAtt->toDouble ();};
	} else if (orderingAttType->promotableToString ()) {
		return [lhAtt, rhAtt] {return lhAtt->toString () < rhAtt->toString ();};
	} else {
		cout << "This is bad... cannot do anything with the >.\n";
		exit (1);
	}
}

#endif

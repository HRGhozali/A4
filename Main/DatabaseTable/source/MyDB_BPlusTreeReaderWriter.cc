
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

    // Add dummy PageReaderWriter to pageList if empty
    if (pageList.size() == 0) {
        pageList.push_back(MyDB_PageReaderWriter (*(this->getBufferMgr())));
    }

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

    return make_shared<MyDB_PageListIteratorSelfSortingAlt>(pageList, lhs, rhs, comparator, myRec, lowComparator, highComparator, true);
}


MyDB_RecordIteratorAltPtr MyDB_BPlusTreeReaderWriter :: getRangeIteratorAlt (MyDB_AttValPtr low, MyDB_AttValPtr high) {  // REQUIRED

    vector<MyDB_PageReaderWriter> pageList;

    // check for page count
    // if page count = 0, add a single anonymous page into list
    if (getNumPages() == 0) {
        (*this)[0];  
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
         MyDB_AttValPtr negInfAtt = orderingAttType->createAtt();
        if (orderingAttType->promotableToInt() || orderingAttType->promotableToDouble()) {
            negInfAtt->fromInt(INT_MIN);
        } else if (orderingAttType->promotableToString()) {
            std::string smallest = "";  // smallest possible string
            negInfAtt->fromString(smallest); 
        }

        
        MyDB_INRecordPtr prevRec = getINRecord();
        prevRec->setKey(negInfAtt); 
        prevRec->recordContentHasChanged();
        
        
        // This flag will track whether the program has hit the parent-of-leaves level.
        bool atParentOfLeaves = false;

        // --- Iterate through internal records in sorted (in-order) sequence ---
        while (iter->advance()) {

            iter->getCurrent(currentRec);

            // The child pointer associated with the interval (prevKey, currKey]
            int childPtr = currentRec->getPtr();

            // Compare to the range bounds
            bool currLessLow = buildComparator(currentRec, lowRec)();   // currKey < low?
            bool highLessPrev = buildComparator(highRec, prevRec)();    // high < prevKey?

            // If this child overlaps the query range, explore it
            if (!currLessLow && !highLessPrev) {
                if (atParentOfLeaves) {
                    // Optimization: if we already know children are leaves,
                    // directly add the child page instead of recursing again
                    list.push_back((*this)[childPtr]);
                } else {
                    // Otherwise, recurse normally
                    bool childWasLeaf = discoverPages(childPtr, list, low, high);

                    // If this child was a leaf, mark that this is the leaf-parent level
                    if (childWasLeaf)
                        atParentOfLeaves = true;
                }
            }

            // Advance prevRec for the next iteration
            prevRec->setKey(this->getKey(currentRec));
            prevRec->recordContentHasChanged();
        }
       

        return false; // internal node
    }
    return false; // should not reach here
}


// append a record to the B+-Tree
void MyDB_BPlusTreeReaderWriter::append(MyDB_RecordPtr appendMe) {

    // Create empty BPlusTree
    if (getNumPages() <= 1) { //getTable()->lastPage() <= 0
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


// splits the given page (plus the record andMe) around the median.  A MyDB_INRecordPtr is returned that
// points to the record holding the (key, ptr) pair pointing to the new page.  Note that the new page
// always holds the lower 1/2 of the records on the page; the upper 1/2 remains in the original page
MyDB_RecordPtr MyDB_BPlusTreeReaderWriter :: split (MyDB_PageReaderWriter splitMe, MyDB_RecordPtr andMe) {

    // Claim a new page.
    MyDB_PageType splitPageType = splitMe.getType();
    int newPageIdx = getTable()->lastPage() + 1;
    MyDB_PageReaderWriter newPage = (*this)[newPageIdx];
    newPage.setType(splitPageType);

    // Vector to hold all the records. 
    vector<MyDB_RecordPtr> records;

    // Get iterator over the page to split.
    MyDB_RecordIteratorAltPtr iter = splitMe.getIteratorAlt();

    // Empty record type.
    MyDB_RecordPtr currentRec = (splitPageType == MyDB_PageType::RegularPage) ? getEmptyRecord() : getINRecord();

    while(iter->advance()) {
        iter->getCurrent(currentRec);

        // If splitting a leaf, make a copy a regular record.
        // If splitting an internal node, make a copy of an INRecord.
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

    // Sort records vector
    std::stable_sort(records.begin(), records.end(),[&](const MyDB_RecordPtr& r1, const MyDB_RecordPtr& r2) {
        return buildComparator(r1, r2)();
    });

    // Clear the page to split and reset its type.
    splitMe.clear();
    splitMe.setType(splitPageType);


    /* Splitting logic for both leaf and internal records */
    int midpoint = records.size() / 2;

    // The lower half of records go to the new (left) page.
    for (int i = 0; i < midpoint; i++) {
        newPage.append(records[i]);
    }
    
    // The upper half (INCLUDING the median) goes to the original (right) page.
    for (size_t i = midpoint; i < records.size(); i++) {
        splitMe.append(records[i]);
    }

    // Create a NEW record for promotion.
    MyDB_INRecordPtr promotedRec = getINRecord();
    
    // The promoted key is the LARGEST key in the new (left) page.
    promotedRec->setKey(getKey(records[midpoint - 1]));
    
    // The pointer in the parent must point to this new page containing the smaller keys.
    promotedRec->setPtr(newPageIdx);
    promotedRec->recordContentHasChanged();
    return promotedRec;
}



// appends a record to the named page; if there is a split, then an MyDB_INRecordPtr is returned that
// points to the record holding the (key, ptr) pair pointing to the new page.  Note that the new page
// always holds the lower 1/2 of the records on the page; the upper 1/2 remains in the original page
MyDB_RecordPtr MyDB_BPlusTreeReaderWriter::append(int whichPage, MyDB_RecordPtr appendMe) {
    
    // Get the current PageReaderWriter
    MyDB_PageReaderWriter currPage = (*this)[whichPage];
    
    // Case 1: The page is an internal (directory) node.
    if (currPage.getType() == MyDB_PageType::DirectoryPage) {
        
        MyDB_RecordIteratorAltPtr iterator = currPage.getIteratorAlt();
        int childPageIdx = -1;

        // Valid, non-null record object before the loop.
        // This object will be reused by the iterator to hold data.
        MyDB_INRecordPtr currINRecord = getINRecord();

        while (iterator->advance()) {
           
            iterator->getCurrent(currINRecord);

            // Where to append appendMe.
            if (!buildComparator(currINRecord, appendMe)()) {
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
        MyDB_RecordPtr newSplitRecord = append(childPageIdx, appendMe);

        // If the child split, handle the new record.
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
            // The record fit. Done.
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


void MyDB_BPlusTreeReaderWriter :: printTree (int whichPage, const std::string &prefix) {  // REQUIRED - need to make test case for this too
    auto page{operator[](whichPage)};
    bool isLeaf{page.getType() == MyDB_PageType::RegularPage};
    MyDB_RecordPtr rec{isLeaf ? getEmptyRecord() : getINRecord()};

    auto iter{page.getIteratorAlt()};
    bool more{iter->advance()};
    while(more) {
        iter->getCurrent(rec);
        more = iter->advance();
        // Visualize leaf page.
        std::cout << prefix << (more ? "├" : "└") << rec << std::endl;
        if (!isLeaf) {
            printTree(std::static_pointer_cast<MyDB_INRecord>(rec)->getPtr(), more ? prefix + "|" : prefix + " ");
        }
    }
}

void MyDB_BPlusTreeReaderWriter :: printTree() {
    if (getNumPages() <= 1) {
        cout << "Empty tree" << endl;
        return;
    }
    cout << "Root" << endl;
    printTree(rootLocation, "");

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

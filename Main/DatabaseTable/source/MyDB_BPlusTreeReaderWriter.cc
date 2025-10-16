
#ifndef BPLUS_C
#define BPLUS_C

#include "MyDB_INRecord.h"
#include "MyDB_BPlusTreeReaderWriter.h"
#include "MyDB_PageReaderWriter.h"
#include "MyDB_PageListIteratorSelfSortingAlt.h"
#include "RecordComparator.h"
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
	
	
	// Check if the root location is -1 to detect a new tree.
	if (rootLocation == -1) {

		// This is is a new B+-Tree, so set up a root and first leaf page.
		rootLocation = 0;
		int leafLocation = 1;

		// Create the root (internal) page
        auto rootPage = (*this)[rootLocation];
        rootPage.clear();
        rootPage.setType(MyDB_PageType::DirectoryPage);

        // Create the first leaf page
        auto leafPage = (*this)[leafLocation];
        leafPage.clear();
        leafPage.setType(MyDB_PageType::RegularPage);

        // Create inf record and pointer from root to leaf 
        MyDB_INRecordPtr infRec = getINRecord();  // creates key = inf
        infRec->setPtr(leafLocation);             // pointer to first leaf

        // Append to root internal page
        rootPage.append(infRec);

        // Persist table metadata 
        getTable()->setRootLocation(rootLocation);
        getTable()->setLastPage(leafLocation); // record the last allocated page
	}
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
    MyDB_INRecordPtr hhigh = getINRecord();
    hhigh->setKey(high);

    // // build the comparison functions
    function <bool ()> comparator = buildComparator(lhs, rhs);
    function <bool ()> lowComparator = buildComparator(myRec, llow);
    function <bool ()> highComparator = buildComparator(hhigh, myRec);

	return make_shared<MyDB_PageListIteratorSelfSortingAlt>(pageList, lhs, rhs, comparator, myRec, lowComparator, highComparator, false);
}


// bool MyDB_BPlusTreeReaderWriter :: discoverPages (int whichPage, vector <MyDB_PageReaderWriter> &list, MyDB_AttValPtr low, MyDB_AttValPtr high) {
// 	// Recursively traverse all nodes to find all pages in range
//     MyDB_INRecordPtr llow = getINRecord();
//     llow->setKey(low);
//     MyDB_INRecordPtr hhigh = getINRecord();
//     hhigh->setKey(high);
// 	MyDB_INRecordPtr myRec;
// 	function <bool ()> lowComparator = buildComparator(myRec, llow);
//     function <bool ()> highComparator = buildComparator(hhigh, myRec);

// 	MyDB_PageReaderWriter currPage = (*this)[whichPage];
// 	// Do we need an extra check for whether the page has data that fits? Not sure
// 	if (currPage.getType() == MyDB_PageType::RegularPage) {
// 		return true;
// 	}

// 	// Crappy recursive attempt
// 	MyDB_RecordIteratorAltPtr temp = currPage.getIteratorAlt();
// 	int childPageIdx = -1;
// 	do {
// 		temp->getCurrent(myRec);  // I do not know if my traversal stuff works
// 		if (lowComparator() && highComparator()) {
// 			childPageIdx = myRec->getPtr();
// 			if (discoverPages(childPageIdx, list, low, high)) {  // If leaf, stop and immediately put every leaf into the list
// 				do {
// 					temp->getCurrent(myRec);
// 					childPageIdx = myRec->getPtr();
// 					list.push_back((*this)[childPageIdx]);
// 				} while (temp->advance());
// 				break;
// 			}
// 		}
// 	} while (temp->advance());
	
// 	return false;
// }

bool MyDB_BPlusTreeReaderWriter::discoverPages(int whichPage, vector<MyDB_PageReaderWriter> &list, MyDB_AttValPtr low, MyDB_AttValPtr high) {

    MyDB_PageReaderWriter currentPage = (*this)[whichPage];

    // Base Case: If this is a leaf page, add it to the list.
    if (currentPage.getType() == MyDB_PageType::RegularPage) {
        list.push_back(currentPage);
        return true;
    }

    // Recursive Step: If this is an internal page, explore its children.
    if (currentPage.getType() == MyDB_PageType::DirectoryPage) {
        MyDB_RecordIteratorAltPtr iter = currentPage.getIteratorAlt();
        while (iter->advance()) {
            MyDB_INRecordPtr currentRec;
            iter->getCurrent(currentRec);
            
            // Recursively call discoverPages on the child pointer.
            // For a full traversal, we visit every child.
            discoverPages(currentRec->getPtr(), list, low, high);
        }
        return false;
    }
    
    return false; // Should not be reached
}

void MyDB_BPlusTreeReaderWriter::append(MyDB_RecordPtr appendMe) {
    
    // Call the private, recursive append method, starting from the root.
    MyDB_RecordPtr newSplitRecord = append(rootLocation, appendMe);

    // If the private append returns a non-null pointer, it means the root itself has split.
    if (newSplitRecord != nullptr) {
        
        // 1. Get a new page from the buffer manager to serve as the new root.
        int newRootIndex = getTable()->lastPage() + 1;

        // Immediately update the table's metadata to claim this new page.
        getTable()->setLastPage(newRootIndex);

        MyDB_PageReaderWriter newRootPage = (*this)[newRootIndex];
        newRootPage.setType(MyDB_PageType::DirectoryPage);

        // 2. Add the record returned by the split, which points to the new page.
        newRootPage.append(newSplitRecord);

        // 3. Create a record that points to the OLD root page.
        // The key for this record is "infinity" by default, which is correct for
        // the rightmost pointer in a B+-Tree internal node.
        MyDB_INRecordPtr oldRootPtrRec = getINRecord();
        oldRootPtrRec->setPtr(rootLocation);
        newRootPage.append(oldRootPtrRec);

        // 4. Update the tree's metadata to make the new page the official root.
        rootLocation = newRootIndex;
        getTable()->setRootLocation(rootLocation);
    }
}


MyDB_RecordPtr MyDB_BPlusTreeReaderWriter :: split (MyDB_PageReaderWriter splitMe, MyDB_RecordPtr andMe) {
    // 1. Allocate a new page.

    
    // Get the index for the new page.
    int newPageIdx = getTable()->lastPage() + 1;
    
    // Immmediatly claim this page.
    getTable()->setLastPage(newPageIdx);

    // Get a PageReaderWriter for the new page.
    MyDB_PageReaderWriter newPage = (*this)[newPageIdx];

    // Set the new page's type to match the original page's type
    MyDB_PageType splitPageType = splitMe.getType();
    newPage.setType(splitPageType);

    // 2. Gather and sort all record
    
    vector<MyDB_RecordPtr> records;

    // Use a page iterator to loop through all records in splitMe and add them to the vector.
    MyDB_RecordIteratorAltPtr iter = newPage.getIteratorAlt();
    while(iter->advance() {
        MyDB_INRecordPtr currentRec;
        iter->getCurrent(currentRec);

        records.push_back(currentRec);
    })

    // Add the new record (andMe) to the vector.
    records.push_back(andMe);

    // Sort the vector
    std::sort(records.begin(), ecords.end(),[&](const MyDB_RecordPtr& r1, const MyDB_RecordPtr& r2) {
        // The lambda captures 'this' to call the member function buildComparator.
        // It returns true if r1 should come before r2, and false otherwise.
        return buildComparator(r1, r2)();
    });

    // Clear the original page
    splitMe.clear();

    // Calculate the split point
    int midpoint = records.size() / 2;
    
    
    // -- Case 1: Splitting a leaf page (same height)

    if (splitPageType == MyDB_PageType::RegularPage) {

       
        // The first half goes to the new page
        for (int i = 0l i < midpoint; i++) {
            newPage.append(records[i]);
        }

        // The second half goes back to the original page
        for (int i = midpoint; i < records.size(); i++) {
            splitMe.append(records[i]);
        }

        // The promoted key is the first key in the second half
        MyDB_INRecordPtr promotedRec = getINRecord();
        promotedRec->setkey(getKey(records[midpoint]));
        return promotedRec;
    } 
    // Case 2: Internal/Directory pages (height growth)
    else {

        // Isolate the middle record.
        MyDB_RecordPtr middleRecord = records[midpoint];

        // The first half goes to the new page
        for (int i = 0; i < midpoint; i++) {
            newPage.append(records[i]);
        }

        // The second half (excluding the middle) goes back to the original page.
        for (int i = midpoint + 1 < allRecords.size(); i++) {
            splitMe.append(records[i]);
        }

        // return MyDB_INRecordPtr
        MyDB_INRecordPtr promotedRec = getINRecord();
        promotedRec->setkey(getKey(middleRecord));
        return promotedRec;


    }

	return nullptr;
}

// appends a record to the named page; if there is a split, then an MyDB_INRecordPtr is returned that
// points to the record holding the (key, ptr) pair pointing to the new page.  Note that the new page
// always holds the lower 1/2 of the records on the page; the upper 1/2 remains in the original page
MyDB_RecordPtr MyDB_BPlusTreeReaderWriter::append(int whichPage, MyDB_RecordPtr appendMe) {
    
    // Get a handle to the current page.
    MyDB_PageReaderWriter currPage = (*this)[whichPage];
    
    // Case 1: The current page is an internal (directory) node.
    if (currPage.getType() == MyDB_PageType::DirectoryPage) {
        
        // Find the correct child pointer to follow.
        MyDB_RecordIteratorAltPtr iterator = currPage.getIteratorAlt();
        int childPageIdx = -1;

        while (iterator->advance()) {
            MyDB_INRecordPtr currINRecord;
            iterator->getCurrent(currINRecord);

            // The comparator returns true if appendMe's key < current record's key.
            // If we find such a key, we've found the correct pointer to follow.
            if (buildComparator(appendMe, currINRecord)()) {
                childPageIdx = currINRecord->getPtr();
                break;
            }

            // If the new key is >= the current key, we continue. We tentatively
            // store this pointer; if the loop finishes, this will be the pointer
            // from the last record, which is the one we need.
            childPageIdx = currINRecord->getPtr();
        }

        // This check ensures a valid child was found. In a correctly formed
        // B+-Tree, an internal page should never be empty.
        if (childPageIdx == -1) {
            return nullptr;
        }

        // Recursively call append on the child page we found.
        MyDB_RecordPtr newSplitRecord = append(childPageIdx, appendMe);

        // If the recursive call returned a record, it means the child page split.
        if (newSplitRecord != nullptr) {
            
            // Try to append the new internal record (from the split) to the current page.
            if (currPage.append(newSplitRecord)) {
                // The record fit. The split is "absorbed" at this level.
                // We must sort the internal page to maintain key order.
                MyDB_INRecordPtr tempR1 = getINRecord();
                MyDB_INRecordPtr tempR2 = getINRecord();
                currPage.sortInPlace(buildComparator(tempR1, tempR2), tempR1, tempR2);

                // Signal that no further splits need to be propagated upwards.
                return nullptr;
            } else {
                // This page is also full. It must also split.
                // Return the result of the split to the parent.
                return split(currPage, newSplitRecord);
            }
        } else {
            // The child did not split, so we are done at this level.
            return nullptr;
        }

    // Case 2: The current page is a leaf node.
    } else {
        // Try to append the data record directly to this leaf page.
        if (currPage.append(appendMe)) {
            // The record fit, so no split is needed.
            return nullptr;
        } else {
            // The page is full, so it must be split.
            return split(currPage, appendMe);
        }
    }
}

MyDB_INRecordPtr MyDB_BPlusTreeReaderWriter :: getINRecord () {
	return make_shared <MyDB_INRecord> (orderingAttType->createAttMax ());
}

void MyDB_BPlusTreeReaderWriter :: printTree () {  // REQUIRED - need to make test case for this too
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

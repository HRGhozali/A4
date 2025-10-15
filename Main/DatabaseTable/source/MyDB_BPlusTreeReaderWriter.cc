
#ifndef BPLUS_C
#define BPLUS_C

#include "MyDB_INRecord.h"
#include "MyDB_BPlusTreeReaderWriter.h"
#include "MyDB_PageReaderWriter.h"
#include "MyDB_PageListIteratorSelfSortingAlt.h"
#include "RecordComparator.h"

MyDB_BPlusTreeReaderWriter :: MyDB_BPlusTreeReaderWriter (string orderOnAttName, MyDB_TablePtr forMe, 
	MyDB_BufferManagerPtr myBuffer) : MyDB_TableReaderWriter (forMe, myBuffer) {

	// find the ordering attribute
	auto res = forMe->getSchema ()->getAttByName (orderOnAttName);

	// remember information about the ordering attribute
	orderingAttType = res.second;
	whichAttIsOrdering = res.first;

	// and the root location
	rootLocation = getTable ()->getRootLocation ();

	// Check the tablels last page to see if it is a new tree
	if (getTable()->lastPage() == -1) {

		// This is is a new B+-Tree, so set up a root and first leaf page.
		rootLocation = 0;

		// Get a page for the root and another for the first leaf
		MyDB_PageReaderWriter rootPage = make_shared<MyDB_PageReaderWriter>(*this, rootLocation);
		MyDB_PageReaderWriter leafPage = make_shared<MyDB_PageReaderWriter>(*this, 1);

		// Set up the corresponding page types
		rootPage.setType(MyDB_PageType::DirectoryPage);
		leafPage.setType(MyDB_PageType::RegularPage);

		// Create an internal record that points to the new leaf page
		MyDB_INRecordPtr initialRec = getINRecord();
		initialRec->setPtr(1);

		// Add this internal record to the root page.
		rootPage.append(initialRec);

		// save the root's last location in the table metadata on disk.
		getTable()->setRootLocation(rootLocation);
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
		this[0];  // ideally makes 1 page
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


bool MyDB_BPlusTreeReaderWriter :: discoverPages (int whichPage, vector <MyDB_PageReaderWriter> &list, MyDB_AttValPtr low, MyDB_AttValPtr high) {
	// Recursively traverse all nodes to find all pages in range
	MyDB_RecordPtr myRec = getEmptyRecord();
    MyDB_INRecordPtr llow = getINRecord();
    llow->setKey(low);
    MyDB_INRecordPtr hhigh = getINRecord();
    hhigh->setKey(high);
	function <bool ()> lowComparator = buildComparator(myRec, llow);
    function <bool ()> highComparator = buildComparator(hhigh, myRec);

	if (make_shared<MyDB_PageReaderWriter>(*this, whichPage)->getType() == MyDB_PageType::RegularPage) {
		return true;
	}

	MyDB_RecordIteratorAltPtr temp = this[whichPage].getIteratorAlt();
	do {
		temp->getCurrent(myRec);
		if (lowComparator() && highComparator()) {
			MyDB_AttValPtr attTemp = getKey(myRec);
			if (discoverPages(attTemp->toInt(), list, low, high)) {
				do {
					temp->getCurrent(myRec);
					MyDB_AttValPtr attTemp2 = getKey(myRec);
					list.push_back(MyDB_PageReaderWriter(*this, attTemp2->toInt()));
				} while (temp->advance());
				break;
			}
		}
	} while (temp->advance());
	
	return false;
}

void MyDB_BPlusTreeReaderWriter :: append (MyDB_RecordPtr) {
	// Split
	// Append (the optional ver.)
	// 1. Append to leaf
	// 2. Query function
}

MyDB_RecordPtr MyDB_BPlusTreeReaderWriter :: split (MyDB_PageReaderWriter, MyDB_RecordPtr) {
	return nullptr;
}

// appends a record to the named page; if there is a split, then an MyDB_INRecordPtr is returned that
// points to the record holding the (key, ptr) pair pointing to the new page.  Note that the new page
// always holds the lower 1/2 of the records on the page; the upper 1/2 remains in the original page
MyDB_RecordPtr append (int whichPage, MyDB_RecordPtr appendMe) {
	
	// Ger a PageReaderWriter for the current page
	MyDB_PageReaderWriter currPage = make_shared<MyDB_PageReaderWriter>(*this, whichPage);

	// Case 1: The current page is an internal diretory node
	if (currPage.getType() == MyDB_PageType::DirectoryPage) {
		
		// Find the correct child ptr to follow by iterating through the internal records.
		// We are looking for the first key that is greater than the key of the record we want to append.
		MyDB_RecordIteratorAltPtr iterator = currPage.getIteratorAlt();
		int childPageIdx = -1;

		while (iterator->advance()) {
			MyDB_INRecordPtr currINRecord;
			iterator->getCurrent(currINRecord);

			// buildComparator returns true if apppendMe's key < currentINRecord's key
			if (buildComparator(appendMe, currentINRecord)()) {
				childPageIdx = currentINRecord->getPtr();
				break;
			}
		}

		// Recursively call append on the child page found
		MyDB_RecordPtr newSplitRecord = append(childPageIdx, appendMe);

		// If the recursive call returned a MyDB_INRecrodPtr, then the child page split
		if (newSplitRecord != nullptr) {

			// Try to append the new internal record (from the split) to the current page
			if (currentPage.append(newSplitRecord)) {
				// The record fit. Sort the internal page to maintain order.
				MyDB_INRecordPtr tempR1 = getINRecord();
				MyDB_INRecordPtr tempR2 = getINRecord();
				currPage.sortInPlace(buildComparator(tempR1, tempR2), tempR1, tempR2);
			} else {
				// The record did not fit, so this internal page must also split.
				return split(currentPage, newSplitRecord);
			}
 		} else {
			// The child did not split, so we are good to go.
			return nullptr;
		}

	// Case 2: The current page is a leaf node.
	} else {
		// Try to append the data record directly to the leaf page.
		// Leaf pages are not kept in sorted order - just append at the end.

		if (currPage.append(appendMe)) {
			// The record fit. No split needed, we are good.
			return nullptr;
		} else {
			// The page is full, must split.
			// Split() will create a new page and return the new internal record to be inserted into the parent node. 
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

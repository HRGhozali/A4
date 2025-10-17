
#ifndef BPLUS_TEST_H
#define BPLUS_TEST_H

#include "MyDB_AttType.h"  
#include "MyDB_BufferManager.h"
#include "MyDB_Catalog.h"  
#include "MyDB_Page.h"
#include "MyDB_PageReaderWriter.h"
#include "MyDB_Record.h"
#include "MyDB_Table.h"
#include "MyDB_TableReaderWriter.h"
#include "MyDB_BPlusTreeReaderWriter.h"
#include "MyDB_Schema.h"
#include "QUnit.h"
#include "Sorting.h"
#include <iostream>

#define FALLTHROUGH_INTENDED do {} while (0)

int main (int argc, char *argv[]) {

	int start = 1;
	if (argc > 1 && argv[1][0] >= '0' && argv[1][0] <= '9') {
		start = argv[1][0] - '0';
	}

	QUnit::UnitTest qunit(cerr, QUnit::normal);

	// create a catalog
	MyDB_CatalogPtr myCatalog = make_shared <MyDB_Catalog> ("catFile");

	// now make a schema
	MyDB_SchemaPtr mySchema = make_shared <MyDB_Schema> ();
	mySchema->appendAtt (make_pair ("suppkey", make_shared <MyDB_IntAttType> ()));
	mySchema->appendAtt (make_pair ("name", make_shared <MyDB_StringAttType> ()));
	mySchema->appendAtt (make_pair ("address", make_shared <MyDB_StringAttType> ()));
	mySchema->appendAtt (make_pair ("nationkey", make_shared <MyDB_IntAttType> ()));
	mySchema->appendAtt (make_pair ("phone", make_shared <MyDB_StringAttType> ()));
	mySchema->appendAtt (make_pair ("acctbal", make_shared <MyDB_DoubleAttType> ()));
	mySchema->appendAtt (make_pair ("comment", make_shared <MyDB_StringAttType> ()));

	// use the schema to create a table
	MyDB_TablePtr myTable = make_shared <MyDB_Table> ("supplier", "supplier.bin", mySchema);

	cout << "Using small page size.\n";

	switch (start) {
	case 1:
	{
		cout << "TEST 1... creating tree for small table, on suppkey " << flush;
		MyDB_BufferManagerPtr myMgr = make_shared <MyDB_BufferManager> (1024, 128, "tempFile");
		MyDB_BPlusTreeReaderWriter supplierTable ("suppkey", myTable, myMgr);
		supplierTable.loadFromTextFile ("supplier.tbl");

                // there should be 10000 records
                MyDB_RecordPtr temp = supplierTable.getEmptyRecord ();
                MyDB_RecordIteratorAltPtr myIter = supplierTable.getIteratorAlt ();

                int counter = 0;
                while (myIter->advance ()) {
                        myIter->getCurrent (temp);
                        counter++;
                }
		bool result = (counter == 10000);
		if (result)
			cout << "\tTEST PASSED\n";
		else
			cout << "\tTEST FAILED\n";
                QUNIT_IS_TRUE (result);
	}
	FALLTHROUGH_INTENDED;
	case 2:
	{
		cout << "TEST 2... creating tree for small table, on nationkey " << flush;
		MyDB_BufferManagerPtr myMgr = make_shared <MyDB_BufferManager> (1024, 128, "tempFile");
		MyDB_BPlusTreeReaderWriter supplierTable ("nationkey", myTable, myMgr);
		supplierTable.loadFromTextFile ("supplier.tbl");

                // there should be 10000 records
                MyDB_RecordPtr temp = supplierTable.getEmptyRecord ();
                MyDB_RecordIteratorAltPtr myIter = supplierTable.getIteratorAlt ();

                int counter = 0;
                while (myIter->advance ()) {
                        myIter->getCurrent (temp);
                        counter++;
                }
		bool result = (counter == 10000);
		if (result)
			cout << "\tTEST PASSED\n";
		else
			cout << "\tTEST FAILED\n";
                QUNIT_IS_TRUE (result);
	}
	FALLTHROUGH_INTENDED;
	case 3:
	{
		cout << "TEST 3... creating tree for small table, on comment " << flush;
		MyDB_BufferManagerPtr myMgr = make_shared <MyDB_BufferManager> (1024, 128, "tempFile");
		MyDB_BPlusTreeReaderWriter supplierTable ("comment", myTable, myMgr);
		supplierTable.loadFromTextFile ("supplier.tbl");

                // there should be 10000 records
                MyDB_RecordPtr temp = supplierTable.getEmptyRecord ();
                MyDB_RecordIteratorAltPtr myIter = supplierTable.getIteratorAlt ();

                int counter = 0;
                while (myIter->advance ()) {
                        myIter->getCurrent (temp);
                        counter++;
                }
		bool result = (counter == 10000);
		if (result)
			cout << "\tTEST PASSED\n";
		else
			cout << "\tTEST FAILED\n";
                QUNIT_IS_TRUE (result);
	}
	FALLTHROUGH_INTENDED;
	case 4:
	{
		cout << "TEST 4... creating tree for large table, on comment " << flush;
		MyDB_BufferManagerPtr myMgr = make_shared <MyDB_BufferManager> (1024, 128, "tempFile");
		MyDB_BPlusTreeReaderWriter supplierTable ("comment", myTable, myMgr);
		supplierTable.loadFromTextFile ("supplierBig.tbl");

                // there should be 320000 records
                MyDB_RecordPtr temp = supplierTable.getEmptyRecord ();
                MyDB_RecordIteratorAltPtr myIter = supplierTable.getIteratorAlt ();

                int counter = 0;
                while (myIter->advance ()) {
                        myIter->getCurrent (temp);
                        counter++;
                }
		bool result = (counter == 320000);
		if (result)
			cout << "\tTEST PASSED\n";
		else
			cout << "\tTEST FAILED\n";
                QUNIT_IS_TRUE (result);
	}
	FALLTHROUGH_INTENDED;
	case 5:
	{
		cout << "TEST 5... creating tree for large table, on comment asking some queries" << flush;
		MyDB_BufferManagerPtr myMgr = make_shared <MyDB_BufferManager> (1024, 128, "tempFile");
		MyDB_BPlusTreeReaderWriter supplierTable ("comment", myTable, myMgr);
		supplierTable.loadFromTextFile ("supplierBig.tbl");

                // there should be 320000 records
                MyDB_RecordPtr temp = supplierTable.getEmptyRecord ();
                MyDB_RecordIteratorAltPtr myIter = supplierTable.getIteratorAlt ();

		MyDB_StringAttValPtr low = make_shared <MyDB_StringAttVal> ();
		low->set ("slyly ironic");
		MyDB_StringAttValPtr high = make_shared <MyDB_StringAttVal> ();
		high->set ("slyly ironic~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");

		myIter = supplierTable.getRangeIteratorAlt (low, high);
		int counter = 0;
		bool foundIt = false;
       		while (myIter->advance ()) {
       		      	myIter->getCurrent (temp);
			counter++;
			if (temp->getAtt (0)->toInt () == 4171)
				foundIt = true;
       	        }
		bool result = foundIt && (counter = 4192);
		if (result)
			cout << "\tTEST PASSED\n";
		else
			cout << "\tTEST FAILED\n";
                QUNIT_IS_TRUE (result);
	}
	FALLTHROUGH_INTENDED;
	case 6:
	{
		cout << "TEST 6... creating tree for small table, on suppkey, checking for sorted order " << flush;
		MyDB_BufferManagerPtr myMgr = make_shared <MyDB_BufferManager> (1024, 128, "tempFile");
		MyDB_BPlusTreeReaderWriter supplierTable ("suppkey", myTable, myMgr);
		supplierTable.loadFromTextFile ("supplier.tbl");

                // there should be 10000 records
                MyDB_RecordPtr temp = supplierTable.getEmptyRecord ();

                int counter = 0;
		MyDB_IntAttValPtr low = make_shared <MyDB_IntAttVal> ();
		low->set (1);
		MyDB_IntAttValPtr high = make_shared <MyDB_IntAttVal> ();
		high->set (10000);

		MyDB_RecordIteratorAltPtr myIter = supplierTable.getSortedRangeIteratorAlt (low, high);
		bool res = true;
                while (myIter->advance ()) {
                        myIter->getCurrent (temp);
                        counter++;
			if (counter != temp->getAtt (0)->toInt ()) {
				res = false;
				cout << "Found key of " << temp->getAtt (0)->toInt () << ", expected " << counter << "\n";
			}
                }
		if (res && (counter == 10000))
			cout << "\tTEST PASSED\n";
		else
			cout << "\tTEST FAILED\n";
                QUNIT_IS_TRUE (res && (counter == 10000));
	}
	FALLTHROUGH_INTENDED;
	case 7:
	{
		cout << "TEST 7... creating tree for small table, on suppkey, running point queries " << flush;
		MyDB_BufferManagerPtr myMgr = make_shared <MyDB_BufferManager> (1024, 128, "tempFile");
		MyDB_BPlusTreeReaderWriter supplierTable ("suppkey", myTable, myMgr);
		supplierTable.loadFromTextFile ("supplier.tbl");

                // there should be 10000 records
                MyDB_RecordPtr temp = supplierTable.getEmptyRecord ();

		int counter = 0;
		bool res = true;
		for (int i = 1; i < 101; i++) {
			MyDB_IntAttValPtr low = make_shared <MyDB_IntAttVal> ();
			low->set (i * 19);
			MyDB_IntAttValPtr high = make_shared <MyDB_IntAttVal> ();
			high->set (i * 19);
	
			MyDB_RecordIteratorAltPtr myIter = supplierTable.getSortedRangeIteratorAlt (low, high);
			while (myIter->advance ()) {
				myIter->getCurrent (temp);
				counter++;
				if (i * 19 != temp->getAtt (0)->toInt ()) {
					res = false;
					cout << "Found key of " << temp->getAtt (0)->toInt () << ", expected " << i * 19 << "\n";
				}
			}
		}
		if (res && (counter == 100))
			cout << "\tTEST PASSED\n";
		else
			cout << "\tTEST FAILED\n";
		QUNIT_IS_TRUE (res && (counter == 100));
	}
	FALLTHROUGH_INTENDED;
	case 8:
	{
		cout << "TEST 8... creating tree for small table, on comment, running point queries with no answer " << flush;
		MyDB_BufferManagerPtr myMgr = make_shared <MyDB_BufferManager> (1024, 128, "tempFile");
		MyDB_BPlusTreeReaderWriter supplierTable ("comment", myTable, myMgr);
		supplierTable.loadFromTextFile ("supplier.tbl");

                // there should be 10000 records
                MyDB_RecordPtr temp = supplierTable.getEmptyRecord ();

		int counter = 0;
		for (int i = 0; i < 26; i++) {
			MyDB_StringAttValPtr low = make_shared <MyDB_StringAttVal> ();
			char a = 'a' + i;
			low->set (string (&a));
			MyDB_StringAttValPtr high = make_shared <MyDB_StringAttVal> ();
			high->set (string (&a));
	
			MyDB_RecordIteratorAltPtr myIter = supplierTable.getSortedRangeIteratorAlt (low, high);
			while (myIter->advance ()) {
				myIter->getCurrent (temp);
				counter++;
			}
		}
		if (counter == 0)
			cout << "\tTEST PASSED\n";
		else
			cout << "\tTEST FAILED\n";
		QUNIT_IS_TRUE (counter == 0);
	}
	FALLTHROUGH_INTENDED;
	case 9:
	{
		cout << "TEST 9... creating tree for small table, on suppkey, running point queries under and over range " << flush;
		MyDB_BufferManagerPtr myMgr = make_shared <MyDB_BufferManager> (1024, 128, "tempFile");
		MyDB_BPlusTreeReaderWriter supplierTable ("suppkey", myTable, myMgr);
		supplierTable.loadFromTextFile ("supplier.tbl");

                // there should be 10000 records
                MyDB_RecordPtr temp = supplierTable.getEmptyRecord ();

		int counter = 0;
		for (int i = -1; i <= 10001; i += 10002) {
			MyDB_IntAttValPtr low = make_shared <MyDB_IntAttVal> ();
			low->set (i);
			MyDB_IntAttValPtr high = make_shared <MyDB_IntAttVal> ();
			high->set (i);
	
			MyDB_RecordIteratorAltPtr myIter = supplierTable.getSortedRangeIteratorAlt (low, high);
			while (myIter->advance ()) {
				myIter->getCurrent (temp);
				counter++;
			}
		}
		if (counter == 0)
			cout << "\tTEST PASSED\n";
		else
			cout << "\tTEST FAILED\n";
		QUNIT_IS_TRUE (counter == 0);
	}
	FALLTHROUGH_INTENDED;
	case 10:
	{
		cout << "TEST 10... mega test using tons of range queries " << flush;
		MyDB_BufferManagerPtr myMgr = make_shared <MyDB_BufferManager> (1024 * 128, 128, "tempFile");
		MyDB_BPlusTreeReaderWriter supplierTable ("suppkey", myTable, myMgr);

		// load it from a text file
		supplierTable.loadFromTextFile ("supplierBig.tbl");

                // there should be 320000 records
                MyDB_RecordPtr temp = supplierTable.getEmptyRecord ();
                MyDB_RecordIteratorAltPtr myIter = supplierTable.getIteratorAlt ();

		// now, we check 100 different random suppliers queries
		bool allOK = true;
		for (int time = 0; time < 2; time++) {
			for (int i = 0; i < 100; i++) {
	
				// we are looping through twice; the first time, ask only point queries
				srand48 (i);
				int lowBound = lrand48 () % 10000;
				int highBound = lrand48 () % 10000;
				if (time % 2 == 0)
					highBound = lowBound;
	
				// make sure the low bound is less than the high bound
				if (lowBound > highBound) {
					int temp = lowBound;
					lowBound = highBound;
					highBound = temp;
				}
	
				// ask a range query
				MyDB_IntAttValPtr low = make_shared <MyDB_IntAttVal> ();
				low->set (lowBound);
				MyDB_IntAttValPtr high = make_shared <MyDB_IntAttVal> ();
				high->set (highBound);

				if (i % 2 == 0) 
					myIter = supplierTable.getRangeIteratorAlt (low, high);
				else
					myIter = supplierTable.getSortedRangeIteratorAlt (low, high);
		
				// verify we got exactly the correct count back
				int counter = 0;
       		         	while (myIter->advance ()) {
       		                	myIter->getCurrent (temp);
					counter++;
       	         		}
	
				if (counter != 32 * (highBound - lowBound + 1))
					allOK = false;
			}
		}
		if (allOK)
			cout << "\tTEST PASSED\n";
		else
			cout << "\tTEST FAILED\n";
		QUNIT_IS_TRUE (allOK);
	}
	}
}

#endif








// #ifndef BPLUS_TEST_H
// #define BPLUS_TEST_H

// #include "MyDB_AttType.h"  
// #include "MyDB_BufferManager.h"
// #include "MyDB_Catalog.h"  
// #include "MyDB_Page.h"
// #include "MyDB_PageReaderWriter.h"
// #include "MyDB_Record.h"
// #include "MyDB_Table.h"
// #include "MyDB_TableReaderWriter.h"
// #include "MyDB_BPlusTreeReaderWriter.h"
// #include "MyDB_Schema.h"
// #include "QUnit.h"
// #include "Sorting.h"
// #include <iostream>
// #include <cstdio> // Required for the remove() function

// int main (int argc, char *argv[]) {
	
// 	// Add this line to be 100% sure you are running the new version
//     cout << "--- EXECUTING LATEST TEST BUILD ---" << endl;

// 	// **THE FIX**: Remove the old table file to ensure a clean start for every test run.
//     // This prevents stale data from a previous failed run from causing initialization errors.
//     remove("supplier.bin");
// 	remove("catFile");
// 	remove("tempFile");
//     QUnit::UnitTest qunit(cerr, QUnit::normal);

//     // Create a catalog
//     MyDB_CatalogPtr myCatalog = make_shared <MyDB_Catalog> ("catFile");

//     // Now make a schema
//     MyDB_SchemaPtr mySchema = make_shared <MyDB_Schema> ();
//     mySchema->appendAtt (make_pair ("suppkey", make_shared <MyDB_IntAttType> ()));
//     mySchema->appendAtt (make_pair ("name", make_shared <MyDB_StringAttType> ()));
//     mySchema->appendAtt (make_pair ("address", make_shared <MyDB_StringAttType> ()));
//     mySchema->appendAtt (make_pair ("nationkey", make_shared <MyDB_IntAttType> ()));
//     mySchema->appendAtt (make_pair ("phone", make_shared <MyDB_StringAttType> ()));
//     mySchema->appendAtt (make_pair ("acctbal", make_shared <MyDB_DoubleAttType> ()));
//     mySchema->appendAtt (make_pair ("comment", make_shared <MyDB_StringAttType> ()));

    

//     // Use the schema to create a table
//     MyDB_TablePtr myTable = make_shared <MyDB_Table> ("supplier", "supplier.bin", mySchema);

//     cout << "Using small page size.\n";

//     // // Test inserting a single record to avoid splits.
//     {
//         cout << "TEST (No Split)... creating tree for a single record table" << flush;
//         MyDB_BufferManagerPtr myMgr = make_shared <MyDB_BufferManager> (1024, 128, "tempFile");
//         MyDB_BPlusTreeReaderWriter supplierTable ("suppkey", myTable, myMgr);

//         // Load from the single-record file.
//         // Make sure you have a file named "singleSupplier.tbl" with one record.
//         supplierTable.loadFromTextFile ("singleSupplier.tbl");

//         // There should be exactly 1 record in the tree
//         MyDB_RecordPtr temp = supplierTable.getEmptyRecord ();
//         MyDB_RecordIteratorAltPtr myIter = supplierTable.getIteratorAlt ();

//         int counter = 0;
//         while (myIter->advance ()) {
//                 myIter->getCurrent (temp);
//                 counter++;
//         }
        
//         cout << "Found " << counter << " records.\n";
//         bool result = (counter == 1);

//         if (result)
//             cout << "\tTEST PASSED\n";
//         else
//             cout << "\tTEST FAILED\n";

//         QUNIT_IS_TRUE (result);
//     }

//      // Test inserting five records, which should fit on a single leaf page.
//     {
//         cout << "TEST (5 Records, No Split)... creating tree from 5-record table" << flush;
//         MyDB_BufferManagerPtr myMgr = make_shared <MyDB_BufferManager> (1024, 128, "tempFile");
//         MyDB_BPlusTreeReaderWriter supplierTable ("suppkey", myTable, myMgr);

//         // Load from the 5-record file.
//         supplierTable.loadFromTextFile ("tempSupplier.tbl");

//         // There should be exactly 5 records in the tree
//         MyDB_RecordPtr temp = supplierTable.getEmptyRecord ();
//         MyDB_RecordIteratorAltPtr myIter = supplierTable.getIteratorAlt ();

//         int counter = 0;
//         while (myIter->advance ()) {
//                 myIter->getCurrent (temp);
//                 counter++;
//         }
        
//         cout << "Found " << counter << " records.\n";
//         bool result = (counter == 5);

//         if (result)
//             cout << "\tTEST PASSED\n";
//         else
//             cout << "\tTEST FAILED\n";

//         QUNIT_IS_TRUE (result);
//     }

//     //+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//     // TEST 3: Insert enough records to cause the first leaf page to split.
//     //+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//     {
//         cout << "\nTEST 3 (Leaf Split)... triggering a split on a leaf page\n" << flush;
//         remove("supplier.bin"); remove("catFile"); remove("tempFile");

//         MyDB_CatalogPtr myCatalog = make_shared <MyDB_Catalog> ("catFile");
//         MyDB_TablePtr myTable = make_shared <MyDB_Table> ("supplier", "supplier.bin", mySchema);
//         MyDB_BufferManagerPtr myMgr = make_shared <MyDB_BufferManager> (1024, 128, "tempFile");
//         MyDB_BPlusTreeReaderWriter supplierTable ("suppkey", myTable, myMgr);

//         // NOTE: You must create a "splitSupplier.tbl" file with enough records
//         // to cause a split (e.g., 6-10 records).
//         const int numRecordsInSplitFile = 9; // <-- Change this to match your file
//         supplierTable.loadFromTextFile ("splitSupplier.tbl");

//         // Verification 1: Check that all records are present.
//         MyDB_RecordPtr temp = supplierTable.getEmptyRecord ();
//         MyDB_RecordIteratorAltPtr myIter = supplierTable.getIteratorAlt ();
//         int counter = 0;
//         while (myIter->advance ()) { myIter->getCurrent (temp); counter++; }
        
//         cout << "Found " << counter << " records.\n";
//         bool correctCount = (counter == numRecordsInSplitFile);

//         // Verification 2: Check that a new page was created.
//         // Initial tree: root (idx 0), leaf (idx 1) -> lastPage = 1
//         // After split: new leaf (idx 2) -> lastPage = 2
//         int lastPage = supplierTable.getTable()->lastPage();
//         cout << "Table's last page index is: " << lastPage << endl;
//         bool correctPageCount = (lastPage == 2);

//         bool result = correctCount && correctPageCount;
//         if (result) {
//             cout << "\tTEST 3 PASSED\n";
//         } else {
//             cout << "\tTEST 3 FAILED\n";
//             if (!correctCount) cout << "\t\t-> Incorrect record count.\n";
//             if (!correctPageCount) cout << "\t\t-> Incorrect page count after split (expected last page index of 2).\n";
//         }
//         QUNIT_IS_TRUE (result);
//     }

//     //+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//     // TEST 4: Insert enough records to cause the root to split, increasing tree height.
//     //+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//     {
//         cout << "\nTEST 4 (Root Split)... triggering a split on the root page\n" << flush;
//         remove("supplier.bin"); remove("catFile"); remove("tempFile");

//         MyDB_CatalogPtr myCatalog = make_shared <MyDB_Catalog> ("catFile");
//         MyDB_TablePtr myTable = make_shared <MyDB_Table> ("supplier", "supplier.bin", mySchema);
//         MyDB_BufferManagerPtr myMgr = make_shared <MyDB_BufferManager> (1024, 128, "tempFile");
//         MyDB_BPlusTreeReaderWriter supplierTable ("suppkey", myTable, myMgr);

//         // Get the root location BEFORE loading data. It should be 0.
//         int initialRoot = myTable->getRootLocation();
//         cout << "Initial root location: " << initialRoot << endl;

//         // NOTE: You must create "rootSplitSupplier.tbl" with enough records
//         // to cause many leaf splits, which will then fill and split the root.
//         // This is likely several hundred records (e.g., 400).
//         const int numRecordsInRootSplitFile = 400; // <-- Adjust this to match your file
//         supplierTable.loadFromTextFile ("rootSplitSupplier.tbl");

//         // Verification 1: Check that all records are present.
//         MyDB_RecordPtr temp = supplierTable.getEmptyRecord();
//         MyDB_RecordIteratorAltPtr myIter = supplierTable.getIteratorAlt();
//         int counter = 0;
//         while (myIter->advance()) { myIter->getCurrent(temp); counter++; }
        
//         cout << "Found " << counter << " records.\n";
//         bool correctCount = (counter == numRecordsInRootSplitFile);

//         // Verification 2: Check that the root location has changed.
//         // This is the definitive proof that the root split and the tree grew in height.
//         int finalRoot = myTable->getRootLocation();
//         cout << "Final root location: " << finalRoot << endl;
//         bool rootChanged = (finalRoot > initialRoot);

//         bool result = correctCount && rootChanged;
//         if (result) {
//             cout << "\tTEST 4 PASSED\n";
//         } else {
//             cout << "\tTEST 4 FAILED\n";
//             if (!correctCount) cout << "\t\t-> Incorrect record count.\n";
//             if (!rootChanged) cout << "\t\t-> Root location did not change (root did not split).\n";
//         }
//         QUNIT_IS_TRUE (result);
//     }

//     //+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//     // TEST 5: Efficient Range Scan
//     //+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
//     {
//         cout << "\nTEST 5 (Range Scan)... querying a range of keys\n" << flush;
//         remove("supplier.bin"); remove("catFile"); remove("tempFile");

//         MyDB_CatalogPtr myCatalog = make_shared <MyDB_Catalog> ("catFile");
//         MyDB_TablePtr myTable = make_shared <MyDB_Table> ("supplier", "supplier.bin", mySchema);
//         MyDB_BufferManagerPtr myMgr = make_shared <MyDB_BufferManager> (1024, 128, "tempFile");
//         MyDB_BPlusTreeReaderWriter supplierTable ("suppkey", myTable, myMgr);

//         // Load a large number of records to create a reasonably deep tree
//         const int numRecordsInFile = 400;
//         supplierTable.loadFromTextFile("rootSplitSupplier.tbl");

//         // Define the low and high bounds for our range query
//         MyDB_IntAttValPtr low = make_shared<MyDB_IntAttVal>();
//         low->set(100);
//         MyDB_IntAttValPtr high = make_shared<MyDB_IntAttVal>();
//         high->set(150);

//         // Get the iterator for the specified range
//         MyDB_RecordIteratorAltPtr myIter = supplierTable.getRangeIteratorAlt(low, high);

//         // Verification loop
//         MyDB_RecordPtr temp = supplierTable.getEmptyRecord();
//         int counter = 0;
//         bool allInRange = true;
//         while (myIter->advance()) {
//             myIter->getCurrent(temp);
//             counter++;
//             int currentKey = temp->getAtt(0)->toInt();
//             if (currentKey < 100 || currentKey > 150) {
//                 allInRange = false;
//                 cout << "\t\t-> ERROR: Found key " << currentKey << " which is outside the range [100, 150].\n";
//             }
//         }

//         // The expected number of records is high - low + 1
//         int expectedCount = 150 - 100 + 1;
//         cout << "Found " << counter << " records in range. Expected " << expectedCount << ".\n";
        
//         bool correctCount = (counter == expectedCount);
//         bool result = correctCount && allInRange;

//         if (result) {
//             cout << "\tTEST 5 PASSED\n";
//         } else {
//             cout << "\tTEST 5 FAILED\n";
//             if (!correctCount) cout << "\t\t-> Incorrect record count.\n";
//             if (!allInRange) cout << "\t\t-> Found records outside the specified key range.\n";
//         }
//         QUNIT_IS_TRUE(result);
//     }
// }

// #endif
package org.example;

import java.io.Serializable;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class MainTest {

    static class MutableValue implements Serializable {
        private int value;

        public MutableValue(int value) {
            this.value = value;
        }

        public int getValue() {
            return value;
        }

        public void setValue(int value) {
            this.value = value;
        }
    }

    @Test
    public void mutableValue() {
        try (MVStore store = new MVStore.Builder().open()) {
            // Create an MVMap instance within the store
            MVMap<String, MutableValue> map = store.openMap("testMap");

            // Create and insert a mutable object into the map
            MutableValue mutableValue = new MutableValue(10);
            map.put("key1", mutableValue);

            // Modify the object after insertion
            mutableValue.setValue(20);

            // Retrieve the object and verify the modification
            MutableValue retrievedValue = map.get("key1");
            assertNotNull(retrievedValue);
            assertEquals(20, retrievedValue.getValue());
        }
    }

    @Test
    public void mutableValueFile(@TempDir Path tempDir) {
        Path mvStorePath = tempDir.resolve("test.mv");
        try (MVStore store = MVStore.open(mvStorePath.toString())) {
            MVMap<String, MutableValue> map = store.openMap("testMap");

            MutableValue mutableValue = new MutableValue(10);
            map.put("key1", mutableValue);

            store.commit();

            mutableValue.setValue(20);

            MutableValue retrievedValue = map.get("key1");
            assertNotNull(retrievedValue);
            assertEquals(20, retrievedValue.getValue());
        }
        try (MVStore store = MVStore.open(mvStorePath.toString())) {
            MVMap<String, MutableValue> map = store.openMap("testMap");
            MutableValue retrievedValue = map.get("key1");
            assertNotNull(retrievedValue);
            assertEquals(20, retrievedValue.getValue());
        }
    }

    @Test
    public void mutableList(@TempDir Path tempDir) {
        Path mvStorePath = tempDir.resolve("test.mv");
        try (MVStore store = MVStore.open(mvStorePath.toString())) {
            MVMap<String, List<MutableValue>> map = store.openMap("testList");

            MutableValue mutableValue = new MutableValue(10);
            // Need to use "ArrayLsit" instead of "List.of", since ArrayList allows for modification
            List<MutableValue> theList = new ArrayList<>(List.of(mutableValue));
            map.put("key1", theList);

            store.commit();

            mutableValue.setValue(20);
            theList.add(new MutableValue(30));
        }
        try (MVStore store = MVStore.open(mvStorePath.toString())) {
            MVMap<String, List<MutableValue>> map = store.openMap("testList");
            List<MutableValue> retrievedValue = map.get("key1");
            assertNotNull(retrievedValue);
            assertEquals(20, retrievedValue.getFirst().getValue());
            assertEquals(30, retrievedValue.getLast().getValue());
        }
    }

    private record MutableValueRecord(MutableValue value, String string) implements Serializable {
    }

    @Test
    public void mutableListOfRecords(@TempDir Path tempDir) {
        Path mvStorePath = tempDir.resolve("test.mv");
        try (MVStore store = MVStore.open(mvStorePath.toString())) {
            MVMap<String, List<MutableValueRecord>> map = store.openMap("testListOfRecords");

            MutableValue mutableValue = new MutableValue(10);
            // Need to use "ArrayLsit" instead of "List.of", since ArrayList allows for modification
            List<MutableValueRecord> theList = new ArrayList<>(List.of(new MutableValueRecord(mutableValue, "first")));
            map.put("key1", theList);

            store.commit();

            mutableValue.setValue(20);
            theList.add(new MutableValueRecord(new MutableValue(30), "second"));
        }
        try (MVStore store = MVStore.open(mvStorePath.toString())) {
            MVMap<String, List<MutableValueRecord>> map = store.openMap("testListOfRecords");
            List<MutableValueRecord> retrievedValue = map.get("key1");
            assertNotNull(retrievedValue);
            assertEquals(20, retrievedValue.getFirst().value.getValue());
            assertEquals(30, retrievedValue.getLast().value.getValue());
            assertEquals("second", retrievedValue.getLast().string);
        }
    }

    @Test
    public void mutableListOfRecordsLastItemModified(@TempDir Path tempDir) {
        Path mvStorePath = tempDir.resolve("test.mv");
        try (MVStore store = MVStore.open(mvStorePath.toString())) {
            MVMap<String, List<MutableValueRecord>> map = store.openMap("testListOfRecords");

            MutableValue mutableValue = new MutableValue(10);
            // Need to use "ArrayLsit" instead of "List.of", since ArrayList allows for modification
            List<MutableValueRecord> theList = new ArrayList<>(List.of(new MutableValueRecord(mutableValue, "first")));
            map.put("key1", theList);

            store.commit();

            mutableValue.setValue(20);
            MutableValueRecord second = new MutableValueRecord(new MutableValue(30), "second");
            theList.add(second);

            // second.string = "last"; in "long form", because records are not modifable
            MutableValueRecord last = new MutableValueRecord(second.value, "last");
            theList.set(1, last);
        }
        try (MVStore store = MVStore.open(mvStorePath.toString())) {
            MVMap<String, List<MutableValueRecord>> map = store.openMap("testListOfRecords");
            List<MutableValueRecord> retrievedValue = map.get("key1");
            assertNotNull(retrievedValue);
            assertEquals(20, retrievedValue.getFirst().value.getValue());
            assertEquals(30, retrievedValue.getLast().value.getValue());
            assertEquals("last", retrievedValue.getLast().string);
        }
    }
}

package io.keen.client.java;

import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests the FileEventSTore class.
 *
 * @author Kevin Litwack (kevin@kevinlitwack.com)
 * @since 2.0.0
 */
public class FileEventStoreTest extends EventStoreTestBase {

    private static final File TEST_STORE_ROOT = new File("test_store_root");

    @BeforeClass
    public static void createStoreRoot() throws Exception {
        FileUtils.forceMkdir(TEST_STORE_ROOT);
    }

    @Before
    public void cleanStoreRoot() throws IOException {
        FileUtils.cleanDirectory(TEST_STORE_ROOT);
    }

    @AfterClass
    public static void deleteStoreRoot() throws Exception {
        FileUtils.deleteDirectory(TEST_STORE_ROOT);
    }

    @Override
    protected KeenEventStore buildStore() throws IOException {
        return new FileEventStore(TEST_STORE_ROOT);
    }

    @Test
    public void existingEventFilesFound() throws Exception {
        writeEventFile("keen/project1/collection1/1393564454103.0", TEST_EVENT_1);
        writeEventFile("keen/project1/collection1/1393564454104.0", TEST_EVENT_2);
        Map<String, List<Object>> handleMap = store.getHandles("project1", 100);
        assertNotNull(handleMap);
        assertEquals(1, handleMap.size());
        List<Object> handles = handleMap.get("collection1");
        assertNotNull(handles);
        assertEquals(2, handles.size());
        List<String> events = new ArrayList<String>();
        for (Object handle : handles) {
            events.add(store.get(handle));
        }
        assertTrue(events.contains(TEST_EVENT_1));
        assertTrue(events.contains(TEST_EVENT_2));
    }

    @Test
    public void existingLimitedEventFilesFound() throws Exception {
        writeEventFile("keen/project1/collection1/1393564454103.0", TEST_EVENT_1);
        writeEventFile("keen/project1/collection1/1393564454104.0", TEST_EVENT_2);
        Map<String, List<Object>> handleMap = store.getHandles("project1", 1);
        assertNotNull(handleMap);
        assertEquals(1, handleMap.size());
        List<Object> handles = handleMap.get("collection1");
        assertNotNull(handles);
        assertEquals(1, handles.size());
        List<String> events = new ArrayList<String>();
        for (Object handle : handles) {
            events.add(store.get(handle));
        }
        assertTrue(events.contains(TEST_EVENT_1));
    }

    @Test
    public void existingLimitedEventFilesFoundInDifferentDir0() throws Exception {
        writeEventFile("keen/project1/collection1/1393564454103.0", TEST_EVENT_1);
        writeEventFile("keen/project1/collection1/1393564454104.0", TEST_EVENT_2);
        writeEventFile("keen/project1/collection2/1393564454105.0", TEST_EVENT_3);
        writeEventFile("keen/project1/collection2/1393564454106.0", TEST_EVENT_4);
        Map<String, List<Object>> handleMap = store.getHandles("project1", 3);
        assertNotNull(handleMap);
        assertEquals(2, handleMap.size());
        {
            List<Object> handles = handleMap.get("collection1");
            assertNotNull(handles);
            assertEquals(2, handles.size());
            assertTrue(handles.get(0).toString().endsWith("keen/project1/collection1/1393564454103.0"));
            assertTrue(handles.get(1).toString().endsWith("keen/project1/collection1/1393564454104.0"));
        }
        {
            List<Object> handles = handleMap.get("collection2");
            assertNotNull(handles);
            assertEquals(1, handles.size());
            assertTrue(handles.get(0).toString().endsWith("keen/project1/collection2/1393564454105.0"));
        }
    }

    @Test
    public void existingLimitedEventFilesFoundInDifferentDir1() throws Exception {
        writeEventFile("keen/project1/collection1/1393564454100.0", TEST_EVENT_1);
        writeEventFile("keen/project1/collection1/1393564454101.0", TEST_EVENT_1);
        writeEventFile("keen/project1/collection1/1393564454102.0", TEST_EVENT_1);
        writeEventFile("keen/project1/collection2/1393564454103.0", TEST_EVENT_1);
        writeEventFile("keen/project1/collection2/1393564454104.0", TEST_EVENT_1);
        writeEventFile("keen/project1/collection2/1393564454105.0", TEST_EVENT_1);
        writeEventFile("keen/project1/collection3/1393564454106.0", TEST_EVENT_1);
        writeEventFile("keen/project1/collection3/1393564454107.0", TEST_EVENT_1);
        writeEventFile("keen/project1/collection3/1393564454108.0", TEST_EVENT_1);
        Map<String, List<Object>> handleMap = store.getHandles("project1", 5);
        assertNotNull(handleMap);
        assertEquals(2, handleMap.size());
        {
            List<Object> handles = handleMap.get("collection1");
            assertNotNull(handles);
            assertEquals(3, handles.size());
            assertTrue(handles.get(0).toString().endsWith("keen/project1/collection1/1393564454100.0"));
            assertTrue(handles.get(1).toString().endsWith("keen/project1/collection1/1393564454101.0"));
            assertTrue(handles.get(2).toString().endsWith("keen/project1/collection1/1393564454102.0"));
        }
        {
            List<Object> handles = handleMap.get("collection2");
            assertNotNull(handles);
            assertEquals(2, handles.size());
            assertTrue(handles.get(0).toString().endsWith("keen/project1/collection2/1393564454103.0"));
            assertTrue(handles.get(1).toString().endsWith("keen/project1/collection2/1393564454104.0"));
        }
    }

    @Test
    public void existingLimitedEventFilesFoundInDifferentDir2() throws Exception {
        writeEventFile("keen/project1/collection1/1393564454100.0", TEST_EVENT_1);
        writeEventFile("keen/project1/collection1/1393564454101.0", TEST_EVENT_1);
        writeEventFile("keen/project1/collection1/1393564454102.0", TEST_EVENT_1);
        writeEventFile("keen/project1/collection2/1393564454103.0", TEST_EVENT_1);
        writeEventFile("keen/project1/collection2/1393564454104.0", TEST_EVENT_1);
        writeEventFile("keen/project1/collection2/1393564454105.0", TEST_EVENT_1);
        writeEventFile("keen/project1/collection3/1393564454106.0", TEST_EVENT_1);
        writeEventFile("keen/project1/collection3/1393564454107.0", TEST_EVENT_1);
        writeEventFile("keen/project1/collection3/1393564454108.0", TEST_EVENT_1);
        Map<String, List<Object>> handleMap = store.getHandles("project1", 6);
        assertNotNull(handleMap);
        assertEquals(2, handleMap.size());
        {
            List<Object> handles = handleMap.get("collection1");
            assertNotNull(handles);
            assertEquals(3, handles.size());
            assertTrue(handles.get(0).toString().endsWith("keen/project1/collection1/1393564454100.0"));
            assertTrue(handles.get(1).toString().endsWith("keen/project1/collection1/1393564454101.0"));
            assertTrue(handles.get(2).toString().endsWith("keen/project1/collection1/1393564454102.0"));
        }
        {
            List<Object> handles = handleMap.get("collection2");
            assertNotNull(handles);
            assertEquals(3, handles.size());
            assertTrue(handles.get(0).toString().endsWith("keen/project1/collection2/1393564454103.0"));
            assertTrue(handles.get(1).toString().endsWith("keen/project1/collection2/1393564454104.0"));
            assertTrue(handles.get(2).toString().endsWith("keen/project1/collection2/1393564454105.0"));
        }
    }

    @Test
    public void existingLimitedEventFilesFoundInDifferentDir3() throws Exception {
        writeEventFile("keen/project1/collection1/1393564454100.0", TEST_EVENT_1);
        writeEventFile("keen/project1/collection1/1393564454101.0", TEST_EVENT_1);
        writeEventFile("keen/project1/collection1/1393564454102.0", TEST_EVENT_1);
        writeEventFile("keen/project1/collection2/1393564454103.0", TEST_EVENT_1);
        writeEventFile("keen/project1/collection2/1393564454104.0", TEST_EVENT_1);
        writeEventFile("keen/project1/collection2/1393564454105.0", TEST_EVENT_1);
        writeEventFile("keen/project1/collection3/1393564454106.0", TEST_EVENT_1);
        writeEventFile("keen/project1/collection3/1393564454107.0", TEST_EVENT_1);
        writeEventFile("keen/project1/collection3/1393564454108.0", TEST_EVENT_1);
        Map<String, List<Object>> handleMap = store.getHandles("project1", 8);
        assertNotNull(handleMap);
        assertEquals(3, handleMap.size());
        {
            List<Object> handles = handleMap.get("collection1");
            assertNotNull(handles);
            assertEquals(3, handles.size());
            assertTrue(handles.get(0).toString().endsWith("keen/project1/collection1/1393564454100.0"));
            assertTrue(handles.get(1).toString().endsWith("keen/project1/collection1/1393564454101.0"));
            assertTrue(handles.get(2).toString().endsWith("keen/project1/collection1/1393564454102.0"));
        }
        {
            List<Object> handles = handleMap.get("collection2");
            assertNotNull(handles);
            assertEquals(3, handles.size());
            assertTrue(handles.get(0).toString().endsWith("keen/project1/collection2/1393564454103.0"));
            assertTrue(handles.get(1).toString().endsWith("keen/project1/collection2/1393564454104.0"));
            assertTrue(handles.get(2).toString().endsWith("keen/project1/collection2/1393564454105.0"));
        }
        {
            List<Object> handles = handleMap.get("collection3");
            assertNotNull(handles);
            assertEquals(2, handles.size());
            assertTrue(handles.get(0).toString().endsWith("keen/project1/collection3/1393564454106.0"));
            assertTrue(handles.get(1).toString().endsWith("keen/project1/collection3/1393564454107.0"));
        }
    }

    private void writeEventFile(String path, String data) throws IOException {
        File eventFile = new File(TEST_STORE_ROOT, path);
        FileUtils.write(eventFile, data, "UTF-8");
    }

}

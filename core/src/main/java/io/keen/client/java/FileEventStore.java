package io.keen.client.java;

import java.io.*;
import java.util.*;

/**
 * Implementation of the {@link io.keen.client.java.KeenEventStore} interface using the file system
 * to cache events in between queueing and batch posting.
 *
 * @author Kevin Litwack (kevin@kevinlitwack.com)
 * @since 2.0.0
 */
public class FileEventStore implements KeenEventStore {

    ///// PUBLIC CONSTRUCTORS /////

    /**
     * Constructs a new File-based event store.
     *
     * @param root The root directory in which to store queued event files.
     * @throws IOException If the provided {@code root} isn't an existing directory.
     */
    public FileEventStore(File root) throws IOException {
        if (!root.exists() || !root.isDirectory()) {
            throw new IOException("Event store root '" + root + "' must exist and be a directory");
        }

        this.root = root;
    }

    ///// PUBLIC METHODS /////

     public void setPreference(String key, String value) throws IOException {
        File preferenceFile = new File(this.getKeenPreferencesDirectory(), key);

        Writer writer = null;
        try {
            OutputStream out = new FileOutputStream(preferenceFile);
            writer = new OutputStreamWriter(out, ENCODING);
            writer.write(value);
            writer.flush();
        } finally {
            KeenUtils.closeQuietly(writer);
        }
    }

     public String getPreference(String key, String defaultValue) throws IOException {
        File preferenceFile = new File(this.getKeenPreferencesDirectory(), key);

        if (preferenceFile.exists() && preferenceFile.isFile()) {
            return KeenUtils.convertFileToString(preferenceFile);
        } else {
            return defaultValue;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object store(String projectId, String eventCollection,
                        String event) throws IOException {
        // Prepare the collection cache directory.
        File collectionCacheDir = prepareCollectionDir(projectId, eventCollection);

        // Create the cache file.
        Calendar timestamp = Calendar.getInstance();
        File cacheFile = getFileForEvent(collectionCacheDir, timestamp);

        // Write the event to the cache file.
        Writer writer = null;
        try {
            OutputStream out = new FileOutputStream(cacheFile);
            writer = new OutputStreamWriter(out, ENCODING);
            writer.write(event);
        } finally {
            KeenUtils.closeQuietly(writer);
        }

        // Return the file as the handle to use for retrieving/removing the event.
        return cacheFile;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String get(Object handle) throws IOException {
        if (!(handle instanceof File)) {
            throw new IllegalArgumentException("Expected File, but was " + handle.getClass());
        }

        File eventFile = (File) handle;
        if (eventFile.exists() && eventFile.isFile()) {
            return KeenUtils.convertFileToString(eventFile);
        } else {
            return null;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void remove(Object handle) throws IOException {
        if (!(handle instanceof File)) {
            throw new IllegalArgumentException("Expected File, but was " + handle.getClass());
        }

        File eventFile = (File) handle;
        if (eventFile.exists() && eventFile.isFile()) {
            if (eventFile.delete()) {
                KeenLogging.log(String.format(Locale.US, "Successfully deleted file: %s",
                        eventFile.getAbsolutePath()));
            } else {
                KeenLogging.log(String.format(Locale.US,
                        "CRITICAL ERROR: Could not remove event at %s",
                        eventFile.getAbsolutePath()));
            }
        } else {
            KeenLogging.log(String.format(Locale.US, "WARNING: no event found at %s",
                    eventFile.getAbsolutePath()));
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, List<Object>> getHandles(String projectId, int limit) throws IOException {
        File projectDir = getProjectDir(projectId, false);
        if (projectDir.exists() && projectDir.isDirectory()) {
            return getHandlesFromProjectDirectory(projectDir, limit);
        } else {
            return new HashMap<String, List<Object>>();
        }
    }

    ///// PRIVATE CONSTANTS /////

    /**
     * The encoding to use when writing events to files.
     */
    private static final String ENCODING = "UTF-8";

    /**
     * The number of events that can be stored for a single collection before aging them out.
     */
    private static final int MAX_EVENTS_PER_COLLECTION = 10000;

    /**
     * The number of events to drop when aging out.
     */
    private static final int NUMBER_EVENTS_TO_FORGET = 100;

    ///// PRIVATE FIELDS /////

    private final File root;

    ///// PRIVATE METHODS /////

    /**
     * Gets the handle map for all collections in the specified project cache directory.
     *
     * @param projectDir The cache directory for the project.
     * @param limit
     * @return The handle map. See {@link #getHandles(String, int)} for details.
     * @throws IOException If there is an error reading the event files.
     */
    private Map<String, List<Object>> getHandlesFromProjectDirectory(File projectDir, int limit) throws
            IOException {
        File[] collectionDirs = getSubDirectories(projectDir);

        Map<String, List<Object>> handleMap = new HashMap<String, List<Object>>();
        if (collectionDirs != null) {
            // iterate through the directories
            int count = 0;
            for (File directory : collectionDirs) {
                String collectionName = directory.getName();
                File[] files = getFilesInDir(directory);
                if (files != null) {
                    if (count + files.length > limit) {
                        files = Arrays.asList(files).subList(0, limit - count).toArray(new File[] {});
                        count = limit;
                    }
                    else {
                        count += files.length;
                    }
                    if (files.length == 0) {
                        continue;
                    }
                    List<Object> handleList = new ArrayList<Object>();
                    handleList.addAll(Arrays.asList(files));
                    handleMap.put(collectionName, handleList);
                } else {
                    KeenLogging.log("Directory was null while getting event handles: " + collectionName);
                }
            }
        }

        return handleMap;
    }

    /**
     * Gets the root directory of the Keen cache, based on the root directory passed to the
     * constructor of this file store. If necessary, this method will attempt to create the
     * directory.
     *
     * @return The root directory of the cache.
     */
    private File getKeenCacheDirectory() throws IOException {
        File file = new File(root, "keen");
        if (!file.exists()) {
            boolean dirMade = file.mkdirs();
            if (!dirMade) {
                throw new IOException("Could not make keen cache directory at: " + file.getAbsolutePath());
            }
        }
        return file;
    }

    private File getKeenPreferencesDirectory() throws IOException {
        File file = new File(root, "keenpreferences");
        if (!file.exists()) {
            boolean dirMade = file.mkdirs();
            if (!dirMade) {
                throw new IOException("Could not make keenpreferences cache directory at: " + file.getAbsolutePath());
            }
        }
        return file;
    }

    /**
     * Gets an array containing all of the sub-directories in the given parent directory.
     *
     * @param parent The directory from which to get sub-directories.
     * @return An array of sub-directories.
     * @throws IOException If there is an error listing the files in the directory.
     */
    private File[] getSubDirectories(File parent) throws IOException {
        File[] files = parent.listFiles(new FileFilter() { // Can return null if there are no events
            public boolean accept(File file) {
                return file.isDirectory();
            }
        });
        Arrays.sort(files);
        return files;
    }

    /**
     * Gets an array containing all of the files in the given directory.
     *
     * @param dir A directory.
     * @return An array containing all of the files in the given directory.
     */
    private File[] getFilesInDir(File dir) {
        File[] files = dir.listFiles(new FileFilter() {
            public boolean accept(File file) {
                return file.isFile();
            }
        });
        Arrays.sort(files);
        return files;
    }

    /**
     * Gets the cache directory for the given project. Optionally creates the directory if it
     * doesn't exist.
     *
     * @param projectId The project ID.
     * @return The cache directory for the project.
     * @throws IOException
     */
    private File getProjectDir(String projectId, boolean create) throws IOException {
        File projectDir = new File(getKeenCacheDirectory(), projectId);
        if (create && !projectDir.exists()) {
            KeenLogging.log("Cache directory for project '" + projectId + "' doesn't exist. " +
                    "Creating it.");
            if (!projectDir.mkdirs()) {
                throw new IOException("Could not create project cache directory '" +
                        projectDir.getAbsolutePath() + "'");
            }
        }
        return projectDir;
    }

    /**
     * Gets the directory for events in the given collection. Creates the directory (and any
     * necessary parents) if it does not exist already.
     *
     * @param projectId       The project ID.
     * @param eventCollection The name of the event collection.
     * @return The directory for events in the collection.
     */
    private File getCollectionDir(String projectId, String eventCollection) throws IOException {
        File collectionDir = new File(getProjectDir(projectId, true), eventCollection);
        if (!collectionDir.exists()) {
            KeenLogging.log("Cache directory for event collection '" + eventCollection +
                    "' doesn't exist. Creating it.");
            if (!collectionDir.mkdirs()) {
                throw new IOException("Could not create collection cache directory '" +
                        collectionDir.getAbsolutePath() + "'");
            }
        }
        return collectionDir;
    }

    /**
     * Gets the file to use for a new event in the given collection with the given timestamp. If
     * there are multiple events with identical timestamps, this method will use a counter to
     * create a unique file name for each.
     *
     * @param collectionDir The cache directory for the event collection.
     * @param timestamp     The timestamp of the event.
     * @return The file to use for the new event.
     */
    private File getFileForEvent(File collectionDir, Calendar timestamp) throws IOException {
        int counter = 0;
        File eventFile = getNextFileForEvent(collectionDir, timestamp, counter);
        while (eventFile.exists()) {
            eventFile = getNextFileForEvent(collectionDir, timestamp, counter);
            counter++;
        }
        return eventFile;
    }

    /**
     * Gets the file to use for a new event in the given collection with the given timestamp,
     * using the provided counter.
     *
     * @param dir       The directory in which the file should be created.
     * @param timestamp The timestamp to use as the base file name.
     * @param counter   The counter to append to the file name.
     * @return The file to use.
     */
    private File getNextFileForEvent(File dir, Calendar timestamp, int counter) {
        long timestampInMillis = timestamp.getTimeInMillis();
        String name = Long.toString(timestampInMillis);
        return new File(dir, name + "." + counter);
    }

    /**
     * Gets the maximum number of events per collection.
     *
     * @return The maximum number of events per collection.
     */
    private int getMaxEventsPerCollection() {
        return MAX_EVENTS_PER_COLLECTION;
    }

    /**
     * Gets the number of events to discard if the maximum number of events is exceeded.
     *
     * @return The number of events to discard.
     */
    private int getNumberEventsToForget() {
        return NUMBER_EVENTS_TO_FORGET;
    }

    /**
     * Prepares the file cache for the given event collection for another event to be added. This
     * method checks to make sure that the maximum number of events per collection hasn't been
     * exceeded, and if it has, this method discards events to make room.
     *
     * @param projectId       The project ID.
     * @param eventCollection The name of the event collection.
     * @return The prepared cache directory for the given project/collection.
     * @throws IOException If there is an error creating the directory or validating/discarding
     *                     events.
     */
    private File prepareCollectionDir(String projectId, String eventCollection) throws IOException {
        File collectionDir = getCollectionDir(projectId, eventCollection);

        // Make sure the max number of events has not been exceeded in this collection. If it has,
        // delete events to make room.
        File[] eventFiles = getFilesInDir(collectionDir);
        if (eventFiles.length >= getMaxEventsPerCollection()) {
            // need to age out old data so the cache doesn't grow too large
            KeenLogging.log(String.format(Locale.US, "Too many events in cache for %s, " +
                    "aging out old data", eventCollection));
            KeenLogging.log(String.format(Locale.US, "Count: %d and Max: %d",
                    eventFiles.length, getMaxEventsPerCollection()));

            // delete the eldest (i.e. first we have to sort the list by name)
            List<File> fileList = Arrays.asList(eventFiles);
            Collections.sort(fileList, new Comparator<File>() {
                @Override
                public int compare(File file, File file1) {
                    return file.getAbsolutePath().compareToIgnoreCase(file1.getAbsolutePath());
                }
            });
            for (int i = 0; i < getNumberEventsToForget(); i++) {
                File f = fileList.get(i);
                if (!f.delete()) {
                    KeenLogging.log(String.format(Locale.US,
                            "CRITICAL: can't delete file %s, cache is going to be too big",
                            f.getAbsolutePath()));
                }
            }
        }

        return collectionDir;
    }

}
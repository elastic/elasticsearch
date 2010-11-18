package org.elasticsearch.river.couchdb.run;

import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.river.couchdb.CouchDatabase;
import org.elasticsearch.river.couchdb.http.Http;
import org.elasticsearch.river.couchdb.http.UriBuilder;

import java.io.*;
import java.net.URI;

import static org.elasticsearch.common.collect.Lists.*;
import static org.elasticsearch.common.io.Streams.*;
import static org.elasticsearch.river.couchdb.run.Couch.ResourceLoader.*;

public class Couch {

    private TemporaryDirectory directory = null;
    private Process process;
    private URI couchUri;
    private Http http;

    public Couch(Http http) {
        this.http = http;
    }

    public void start() throws Exception {

        directory = new TemporaryDirectory();

        File generatedConfiguration = directory.file("test-config.txt");

        copy(loadText(getClass(),"test-config.txt").replaceAll("\\$DIR\\$", directory.path()).getBytes(),
                generatedConfiguration);

        ProcessBuilder builder = new ProcessBuilder(newArrayList("/usr/bin/couchdb", "-a", generatedConfiguration.getPath()));

        process = builder.start();

        new Chomper(process.getErrorStream()).start();
        new Chomper(process.getInputStream()).start();

        File uriFile = directory.waitFor("couch.uri");
        String str = loadText(new FileInputStream(uriFile)).trim();
        couchUri = new URI(str);
    }

    public URI uri() {
        return couchUri;
    }

    public void stop() throws InterruptedException, IOException {
        process.destroy();
        process.waitFor();
        directory.remove();
    }

    public CouchDatabase createDatabase(String name) throws IOException {

        URI uri = new UriBuilder(couchUri).addPath(name).url();

        Http.HttpResult result = http.put(uri);

        if ( ! result.ok() ) {
            throw new IOException("Cannot create " + name + ", because " + result);
        }

        return new CouchDatabase(http, name, uri);
    }

    public static class ResourceLoader {

        public static String loadText(Class clazz, String resourceName) throws IOException {

            InputStream stream = clazz.getResourceAsStream(resourceName);
            if (stream == null) {
                throw new Defect("Unable to locate " + resourceName + " relative to " + clazz.getSimpleName());
            }
            return loadText(stream);

        }

        public static String loadText(InputStream stream) throws IOException {
            InputStreamReader reader = new InputStreamReader(stream);
            try {
                return copyToString(reader);
            }
            finally {
                reader.close();
            }
        }
    }

    public static class Chomper {

        private final InputStream stream;

        public Chomper(InputStream stream) {
            this.stream = stream;
        }

        public void start() {
            new Thread(new Runner());
        }

        public class Runner implements Runnable {
            @Override public void run() {
                try {
                    int available = 0;
                    while ((available = stream.available()) > 0) {
                        stream.skip(available);
                    }
                }
                catch (IOException e) {
                    // ignore
                }
            }
        }
    }

    public static class TemporaryDirectory {
        private File directory;

        public TemporaryDirectory() throws IOException {
            File file = File.createTempFile("temp", "dir");
            String path = file.getPath();
            if (!file.delete()) {
                throw new Defect("Unable to remove file i just created ( " + file + ")");
            }

            directory = new File(path);

            if (!directory.mkdirs()) {
                throw new Defect("Unable to create temp dir at " + path);
            }
        }

        public String path() {
            return directory.getPath();
        }

        public File file(String name) {
            return new File(directory, name);
        }

        public void remove() throws IOException {
            FileSystemUtils.deleteRecursively(directory);
        }

        public File waitFor(String filename) throws InterruptedException {
            File file = file(filename);

            long start = System.currentTimeMillis();

            while ( ! file.exists() && ( System.currentTimeMillis() < start + 2000 )) {
                Thread.sleep(50);
            }

            if ( ! file.exists() ) {
                throw new Defect("File " + file + " didn't appear in time");
            }

            return file;
        }
    }

}

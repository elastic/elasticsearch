/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.plugins.repository;

import com.google.common.collect.Maps;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.common.unit.TimeValue;

import javax.net.ssl.*;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.net.URLConnection;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

public class DefaultPluginDownloader implements PluginDownloader {

    /**
     * Directory where downloaded files are stored
     */
    private final Path root;

    /**
     * Counter for downloaded files
     */
    private static final AtomicInteger downloadNumber = new AtomicInteger(0);

    /**
     * Default settings for the downloader
     */
    private TimeValue timeout = TimeValue.timeValueMillis(0);
    private String userAgent = null;
    private String elasticsearchVersion = null;

    private final CopyOnWriteArrayList<Listener> listeners = new CopyOnWriteArrayList<>();

    private static final TrustManager[] TRUST_ALL_CERTS = new TrustManager[]{
            new X509TrustManager() {
                public java.security.cert.X509Certificate[] getAcceptedIssuers() {
                    return null;
                }

                public void checkClientTrusted(
                        java.security.cert.X509Certificate[] certs, String authType) {
                }

                public void checkServerTrusted(
                        java.security.cert.X509Certificate[] certs, String authType) {
                }
            }
    };

    private static final HostnameVerifier TRUST_ALL_HOSTS = new HostnameVerifier() {
        public boolean verify(String hostname, SSLSession session) {
            return true;
        }
    };

    public DefaultPluginDownloader(Path root, TimeValue timeout, String userAgent, String elasticsearchVersion) {
        this.root = root;
        this.userAgent = userAgent;
        this.elasticsearchVersion = elasticsearchVersion;
        if (timeout != null) {
            this.timeout = timeout;
        }
    }

    @Override
    public Path download(String source) throws IOException {
        Files.createDirectories(root);

        // Install the all-trusting trust manager
        try {
            SSLContext sc = SSLContext.getInstance("SSL");
            sc.init(null, TRUST_ALL_CERTS, new java.security.SecureRandom());
            HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());
            HttpsURLConnection.setDefaultHostnameVerifier(TRUST_ALL_HOSTS);
        } catch (GeneralSecurityException e) {
            throw new ElasticsearchException("Exception when installing the all-trusting trust manager", e);
        }

        // Destination for the next download
        Path destination = root.resolve(String.valueOf(downloadNumber.get()));
        while (Files.exists(destination)) {
            destination = root.resolve(String.valueOf(downloadNumber.getAndIncrement()));
        }

        DownloadThread dlThread = new DownloadThread(source, destination, listeners, userAgent, elasticsearchVersion);
        try {
            dlThread.setDaemon(true);
            dlThread.start();
            dlThread.join(timeout.millis());

            if (dlThread.isAlive()) {
                throw new ElasticsearchTimeoutException("The download operation took longer than " + timeout + ", stopping it.");
            }
        } catch (InterruptedException e) {
            return null;
        } finally {
            dlThread.closeStreams();
        }

        if (dlThread.wasSuccessful()) {
            return destination;
        }
        return null;
    }

    @Override
    public void addListener(Listener listener) {
        listeners.add(listener);
    }


    private class DownloadThread extends Thread {

        private final String source;
        private final Path destination;
        private Collection<Listener> listeners;

        private final String userAgent;
        private final String elasticsearchVersion;

        private boolean success = false;
        private Exception exception = null;

        private InputStream input = null;
        private OutputStream output = null;
        private URLConnection connection = null;

        private static final int MAX_REDIRECTIONS = 5;
        private Map<String, String> redirections = Maps.newHashMapWithExpectedSize(MAX_REDIRECTIONS);

        DownloadThread(String source, Path destination, Collection<Listener> listeners, String userAgent, String elasticsearchVersion) {
            this.source = source;
            this.destination = destination;
            this.listeners = listeners;
            this.userAgent = userAgent;
            this.elasticsearchVersion = elasticsearchVersion;
        }

        @Override
        public void run() {
            try {
                success = download();
            } catch (Exception e) {
                exception = e;
            }
        }

        private boolean download() throws IOException {
            String current = source;

            if (listeners != null) {
                for (Listener listener : listeners) {
                    listener.onDownloadBegin(current);
                }
            }

            while ((current != null) && !isInterrupted()) {
                try {
                    URL url = URI.create(current).toURL();

                    // Set up the URL connection
                    connection = url.openConnection();
                    if (connection == null) {
                        return false;
                    }

                    connection.setUseCaches(true);
                    connection.setConnectTimeout(5000);

                    if (userAgent != null) {
                        connection.setRequestProperty("User-Agent", userAgent);
                    }
                    if (elasticsearchVersion != null) {
                        // Custom request header to send the current Elasticsearch version
                        // (no X- prefix, see http://tools.ietf.org/html/rfc6648)
                        connection.addRequestProperty("ES-Version", elasticsearchVersion);
                    }

                    if (connection instanceof HttpURLConnection) {
                        ((HttpURLConnection) connection).setInstanceFollowRedirects(false);
                    }

                    // Connect to the remote site (may take some time)
                    connection.connect();

                    // HTTP only: checks for response code
                    if (connection instanceof HttpURLConnection) {
                        HttpURLConnection httpConnection = (HttpURLConnection) connection;
                        int responseCode = httpConnection.getResponseCode();

                        // HTTP response code is 401
                        if (responseCode == HttpURLConnection.HTTP_UNAUTHORIZED) {
                            throw new IOException("HTTP Authorization failure");
                        }

                        // HTTP response code is 301, 302 or 303
                        if ((responseCode == HttpURLConnection.HTTP_MOVED_PERM)
                                || (responseCode == HttpURLConnection.HTTP_MOVED_TEMP)
                                || (responseCode == HttpURLConnection.HTTP_SEE_OTHER)) {

                            String newLocation = httpConnection.getHeaderField("Location");
                            redirections.put(current.toString(), newLocation);

                            // Checks if redirection is allowed
                            if (redirections.size() > MAX_REDIRECTIONS) {
                                StringBuilder message = new StringBuilder("More than ").append(MAX_REDIRECTIONS).append(" times redirected, giving up");

                                for (Map.Entry<String, String> entry : redirections.entrySet()) {
                                    message.append('[').append(entry.getKey()).append("->").append(entry.getValue()).append("]");
                                }
                                throw new IOException(message.toString());
                            }

                            URL redirect = new URL(newLocation);
                            if (listeners != null) {
                                for (Listener listener : listeners) {
                                    listener.onDownloadRedirection(newLocation, redirect);
                                }
                            }
                            current = newLocation;
                            continue;
                        }

                    }

                    // Now, downloads the file

                    // this three attempt trick is to get round quirks in different
                    // Java implementations. Some of them take a few goes to bind
                    // property; we ignore the first couple of such failures.
                    IOException lastException = null;
                    for (int i = 0; i < 3; i++) {
                        try {
                            input = connection.getInputStream();
                            break;
                        } catch (IOException ex) {
                            lastException = ex;
                        }
                    }

                    if (input == null) {
                        if (lastException instanceof FileNotFoundException) {
                            throw new FileNotFoundException(current);
                        }
                        throw new IOException("Can't download " + current, lastException);
                    }

                    boolean finished = false;
                    try {
                        output = Files.newOutputStream(destination);
                        long total = connection.getContentLengthLong();

                        byte[] buffer = new byte[1024 * 100];
                        int length;

                        while (!isInterrupted() && (length = input.read(buffer)) >= 0) {
                            output.write(buffer, 0, length);
                            if (listeners != null) {
                                for (Listener listener : listeners) {
                                    listener.onDownloadTick(current, total, length);
                                }
                            }
                        }
                        if (isInterrupted()) {
                            if (listeners != null) {
                                for (Listener listener : listeners) {
                                    listener.onDownloadTimeout(current, timeout);
                                }
                            }
                        } else {
                            finished = true;
                        }
                    } finally {
                        if (!finished) {
                            // we have started to (over)write dest, but failed.
                            // Try to delete the garbage we'd otherwise leave
                            // behind.
                            IOUtils.closeWhileHandlingException(output, input);
                            IOUtils.deleteFilesIgnoringExceptions(destination);
                        } else {
                            IOUtils.close(output, input);
                        }
                    }

                    if (listeners != null) {
                        for (Listener listener : listeners) {
                            listener.onDownloadEnd(current, destination);
                        }
                    }

                    return true;
                } catch (Exception e) {
                    if (listeners != null) {
                        for (Listener listener : listeners) {
                            listener.onDownloadError(current, e);
                        }
                    }
                    throw e;

                } finally {
                    if (connection != null) {
                        if (connection instanceof HttpURLConnection) {
                            ((HttpURLConnection) connection).disconnect();
                        }
                    }
                }
            }
            return false;
        }


        /**
         * Has the download completed successfully?
         * <p/>
         * <p>Re-throws any exception caught during executaion.</p>
         */
        boolean wasSuccessful() throws IOException {
            if (exception != null) {
                throw new IOException(exception);
            }
            return success;
        }

        /**
         * Closes streams, interrupts the download, may delete the
         * output file.
         */
        void closeStreams() throws IOException {
            interrupt();
            if (success) {
                IOUtils.close(input, output);
            } else {
                IOUtils.closeWhileHandlingException(input, output);
                if (destination != null) {
                    IOUtils.deleteFilesIgnoringExceptions(destination);
                }
            }
            if (connection != null) {
                try {
                    if (connection instanceof HttpURLConnection) {
                        ((HttpURLConnection) connection).disconnect();
                    }
                } catch (Exception e) {
                    // Ignore
                }
            }
        }
    }
}

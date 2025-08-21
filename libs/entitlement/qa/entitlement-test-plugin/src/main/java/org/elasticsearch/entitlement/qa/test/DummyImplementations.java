/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.qa.test;

import jdk.nio.Channels;

import org.elasticsearch.core.SuppressForbidden;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.DatagramSocketImpl;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.ProtocolFamily;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.net.SocketImpl;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.AsynchronousChannelGroup;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.AsynchronousServerSocketChannel;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.nio.channels.DatagramChannel;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.Pipe;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.channels.spi.AbstractSelectableChannel;
import java.nio.channels.spi.AbstractSelector;
import java.nio.channels.spi.AsynchronousChannelProvider;
import java.nio.channels.spi.SelectorProvider;
import java.nio.charset.Charset;
import java.nio.charset.spi.CharsetProvider;
import java.nio.file.AccessMode;
import java.nio.file.CopyOption;
import java.nio.file.DirectoryStream;
import java.nio.file.FileStore;
import java.nio.file.FileSystem;
import java.nio.file.LinkOption;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.FileAttributeView;
import java.nio.file.spi.FileSystemProvider;
import java.security.cert.Certificate;
import java.text.BreakIterator;
import java.text.Collator;
import java.text.DateFormat;
import java.text.DateFormatSymbols;
import java.text.DecimalFormatSymbols;
import java.text.NumberFormat;
import java.text.spi.BreakIteratorProvider;
import java.text.spi.CollatorProvider;
import java.text.spi.DateFormatProvider;
import java.text.spi.DateFormatSymbolsProvider;
import java.text.spi.DecimalFormatSymbolsProvider;
import java.text.spi.NumberFormatProvider;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.spi.CalendarDataProvider;
import java.util.spi.CalendarNameProvider;
import java.util.spi.CurrencyNameProvider;
import java.util.spi.LocaleNameProvider;
import java.util.spi.LocaleServiceProvider;
import java.util.spi.TimeZoneNameProvider;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLSocketFactory;

/**
 * A collection of concrete subclasses that we can instantiate but that don't actually work.
 * <p>
 * A bit like Mockito but way more painful.
 */
class DummyImplementations {

    static class DummyLocaleServiceProvider extends LocaleServiceProvider {

        @Override
        public Locale[] getAvailableLocales() {
            throw unexpected();
        }
    }

    static class DummyBreakIteratorProvider extends BreakIteratorProvider {

        @Override
        public BreakIterator getWordInstance(Locale locale) {
            throw unexpected();
        }

        @Override
        public BreakIterator getLineInstance(Locale locale) {
            throw unexpected();
        }

        @Override
        public BreakIterator getCharacterInstance(Locale locale) {
            throw unexpected();
        }

        @Override
        public BreakIterator getSentenceInstance(Locale locale) {
            throw unexpected();
        }

        @Override
        public Locale[] getAvailableLocales() {
            throw unexpected();
        }
    }

    static class DummyCollatorProvider extends CollatorProvider {

        @Override
        public Collator getInstance(Locale locale) {
            throw unexpected();
        }

        @Override
        public Locale[] getAvailableLocales() {
            throw unexpected();
        }
    }

    static class DummyDateFormatProvider extends DateFormatProvider {

        @Override
        public DateFormat getTimeInstance(int style, Locale locale) {
            throw unexpected();
        }

        @Override
        public DateFormat getDateInstance(int style, Locale locale) {
            throw unexpected();
        }

        @Override
        public DateFormat getDateTimeInstance(int dateStyle, int timeStyle, Locale locale) {
            throw unexpected();
        }

        @Override
        public Locale[] getAvailableLocales() {
            throw unexpected();
        }
    }

    static class DummyDateFormatSymbolsProvider extends DateFormatSymbolsProvider {

        @Override
        public DateFormatSymbols getInstance(Locale locale) {
            throw unexpected();
        }

        @Override
        public Locale[] getAvailableLocales() {
            throw unexpected();
        }
    }

    static class DummyDecimalFormatSymbolsProvider extends DecimalFormatSymbolsProvider {

        @Override
        public DecimalFormatSymbols getInstance(Locale locale) {
            throw unexpected();
        }

        @Override
        public Locale[] getAvailableLocales() {
            throw unexpected();
        }
    }

    static class DummyNumberFormatProvider extends NumberFormatProvider {

        @Override
        public NumberFormat getCurrencyInstance(Locale locale) {
            throw unexpected();
        }

        @Override
        public NumberFormat getIntegerInstance(Locale locale) {
            throw unexpected();
        }

        @Override
        public NumberFormat getNumberInstance(Locale locale) {
            throw unexpected();
        }

        @Override
        public NumberFormat getPercentInstance(Locale locale) {
            throw unexpected();
        }

        @Override
        public Locale[] getAvailableLocales() {
            throw unexpected();
        }
    }

    static class DummyCalendarDataProvider extends CalendarDataProvider {

        @Override
        public int getFirstDayOfWeek(Locale locale) {
            throw unexpected();
        }

        @Override
        public int getMinimalDaysInFirstWeek(Locale locale) {
            throw unexpected();
        }

        @Override
        public Locale[] getAvailableLocales() {
            throw unexpected();
        }
    }

    static class DummyCalendarNameProvider extends CalendarNameProvider {

        @Override
        public String getDisplayName(String calendarType, int field, int value, int style, Locale locale) {
            throw unexpected();
        }

        @Override
        public Map<String, Integer> getDisplayNames(String calendarType, int field, int style, Locale locale) {
            throw unexpected();
        }

        @Override
        public Locale[] getAvailableLocales() {
            throw unexpected();
        }
    }

    static class DummyCurrencyNameProvider extends CurrencyNameProvider {

        @Override
        public String getSymbol(String currencyCode, Locale locale) {
            throw unexpected();
        }

        @Override
        public Locale[] getAvailableLocales() {
            throw unexpected();
        }
    }

    static class DummyLocaleNameProvider extends LocaleNameProvider {

        @Override
        public String getDisplayLanguage(String languageCode, Locale locale) {
            throw unexpected();
        }

        @Override
        public String getDisplayCountry(String countryCode, Locale locale) {
            throw unexpected();
        }

        @Override
        public String getDisplayVariant(String variant, Locale locale) {
            throw unexpected();
        }

        @Override
        public Locale[] getAvailableLocales() {
            throw unexpected();
        }
    }

    static class DummyTimeZoneNameProvider extends TimeZoneNameProvider {

        @Override
        public String getDisplayName(String ID, boolean daylight, int style, Locale locale) {
            throw unexpected();
        }

        @Override
        public Locale[] getAvailableLocales() {
            throw unexpected();
        }
    }

    static class DummyHttpsURLConnection extends HttpsURLConnection {
        DummyHttpsURLConnection() {
            super(null);
        }

        @Override
        public void connect() {
            throw unexpected();
        }

        @Override
        public void disconnect() {
            throw unexpected();
        }

        @Override
        public boolean usingProxy() {
            throw unexpected();
        }

        @Override
        public String getCipherSuite() {
            throw unexpected();
        }

        @Override
        public Certificate[] getLocalCertificates() {
            throw unexpected();
        }

        @Override
        public Certificate[] getServerCertificates() {
            throw unexpected();
        }
    }

    private static class DummySocketImpl extends SocketImpl {
        @Override
        protected void create(boolean stream) {}

        @Override
        protected void connect(String host, int port) {}

        @Override
        protected void connect(InetAddress address, int port) {}

        @Override
        protected void connect(SocketAddress address, int timeout) {}

        @Override
        protected void bind(InetAddress host, int port) {}

        @Override
        protected void listen(int backlog) {}

        @Override
        protected void accept(SocketImpl s) {}

        @Override
        protected InputStream getInputStream() {
            return null;
        }

        @Override
        protected OutputStream getOutputStream() {
            return null;
        }

        @Override
        protected int available() {
            return 0;
        }

        @Override
        protected void close() {}

        @Override
        protected void sendUrgentData(int data) {}

        @Override
        public void setOption(int optID, Object value) {}

        @Override
        public Object getOption(int optID) {
            return null;
        }
    }

    static class DummySocket extends Socket {
        DummySocket() throws SocketException {
            super(new DummySocketImpl());
        }
    }

    static class DummyServerSocket extends ServerSocket {
        DummyServerSocket() {
            super(new DummySocketImpl());
        }
    }

    static class DummyBoundServerSocket extends ServerSocket {
        DummyBoundServerSocket() {
            super(new DummySocketImpl());
        }

        @Override
        public boolean isBound() {
            return true;
        }
    }

    static class DummySSLSocketFactory extends SSLSocketFactory {
        @Override
        public Socket createSocket(String host, int port) {
            throw unexpected();
        }

        @Override
        public Socket createSocket(String host, int port, InetAddress localHost, int localPort) {
            throw unexpected();
        }

        @Override
        public Socket createSocket(InetAddress host, int port) {
            throw unexpected();
        }

        @Override
        public Socket createSocket(InetAddress address, int port, InetAddress localAddress, int localPort) {
            throw unexpected();
        }

        @Override
        public String[] getDefaultCipherSuites() {
            throw unexpected();
        }

        @Override
        public String[] getSupportedCipherSuites() {
            throw unexpected();
        }

        @Override
        public Socket createSocket(Socket s, String host, int port, boolean autoClose) {
            throw unexpected();
        }
    }

    static class DummyDatagramSocket extends DatagramSocket {
        DummyDatagramSocket() {
            super(new DatagramSocketImpl() {
                @Override
                protected void create() {}

                @Override
                protected void bind(int lport, InetAddress laddr) {}

                @Override
                protected void send(DatagramPacket p) {}

                @Override
                protected int peek(InetAddress i) {
                    return 0;
                }

                @Override
                protected int peekData(DatagramPacket p) {
                    return 0;
                }

                @Override
                protected void receive(DatagramPacket p) {}

                @Override
                protected void setTTL(byte ttl) {}

                @Override
                protected byte getTTL() {
                    return 0;
                }

                @Override
                protected void setTimeToLive(int ttl) {}

                @Override
                protected int getTimeToLive() {
                    return 0;
                }

                @Override
                protected void join(InetAddress inetaddr) {}

                @Override
                protected void leave(InetAddress inetaddr) {}

                @Override
                protected void joinGroup(SocketAddress mcastaddr, NetworkInterface netIf) {}

                @Override
                protected void leaveGroup(SocketAddress mcastaddr, NetworkInterface netIf) {}

                @Override
                protected void close() {}

                @Override
                public void setOption(int optID, Object value) {}

                @Override
                public Object getOption(int optID) {
                    return null;
                }

                @Override
                protected void connect(InetAddress address, int port) {}
            });
        }
    }

    private static RuntimeException unexpected() {
        return new IllegalStateException("This method isn't supposed to be called");
    }

    static class DummySelectorProvider extends SelectorProvider {
        @Override
        public DatagramChannel openDatagramChannel() {
            return null;
        }

        @Override
        public DatagramChannel openDatagramChannel(ProtocolFamily family) {
            return null;
        }

        @Override
        public Pipe openPipe() {
            return null;
        }

        @Override
        public AbstractSelector openSelector() {
            return null;
        }

        @Override
        public ServerSocketChannel openServerSocketChannel() {
            return null;
        }

        @Override
        public SocketChannel openSocketChannel() {
            return null;
        }
    }

    static class DummyAsynchronousChannelProvider extends AsynchronousChannelProvider {
        @Override
        public AsynchronousChannelGroup openAsynchronousChannelGroup(int nThreads, ThreadFactory threadFactory) {
            return null;
        }

        @Override
        public AsynchronousChannelGroup openAsynchronousChannelGroup(ExecutorService executor, int initialSize) {
            return null;
        }

        @Override
        public AsynchronousServerSocketChannel openAsynchronousServerSocketChannel(AsynchronousChannelGroup group) {
            return null;
        }

        @Override
        public AsynchronousSocketChannel openAsynchronousSocketChannel(AsynchronousChannelGroup group) {
            return null;
        }
    }

    static class DummyCharsetProvider extends CharsetProvider {
        @Override
        public Iterator<Charset> charsets() {
            return null;
        }

        @Override
        public Charset charsetForName(String charsetName) {
            return null;
        }
    }

    static class DummyFileSystemProvider extends FileSystemProvider {
        @Override
        public String getScheme() {
            return "";
        }

        @Override
        public FileSystem newFileSystem(URI uri, Map<String, ?> env) {
            return null;
        }

        @Override
        public FileSystem getFileSystem(URI uri) {
            return null;
        }

        @Override
        public Path getPath(URI uri) {
            return null;
        }

        @Override
        public SeekableByteChannel newByteChannel(Path path, Set<? extends OpenOption> options, FileAttribute<?>... attrs) {
            return null;
        }

        @Override
        public DirectoryStream<Path> newDirectoryStream(Path dir, DirectoryStream.Filter<? super Path> filter) {
            return null;
        }

        @Override
        public void createDirectory(Path dir, FileAttribute<?>... attrs) {

        }

        @Override
        public void delete(Path path) {

        }

        @Override
        public void copy(Path source, Path target, CopyOption... options) {

        }

        @Override
        public void move(Path source, Path target, CopyOption... options) {

        }

        @Override
        public boolean isSameFile(Path path, Path path2) {
            return false;
        }

        @Override
        public boolean isHidden(Path path) {
            return false;
        }

        @Override
        public FileStore getFileStore(Path path) {
            return null;
        }

        @Override
        public void checkAccess(Path path, AccessMode... modes) {

        }

        @Override
        public <V extends FileAttributeView> V getFileAttributeView(Path path, Class<V> type, LinkOption... options) {
            return null;
        }

        @Override
        public <A extends BasicFileAttributes> A readAttributes(Path path, Class<A> type, LinkOption... options) {
            return null;
        }

        @Override
        public Map<String, Object> readAttributes(Path path, String attributes, LinkOption... options) {
            return Map.of();
        }

        @Override
        public void setAttribute(Path path, String attribute, Object value, LinkOption... options) {

        }
    }

    static class DummyFileChannel extends FileChannel {
        @Override
        protected void implCloseChannel() {

        }

        @Override
        public int read(ByteBuffer dst) {
            return 0;
        }

        @Override
        public long read(ByteBuffer[] dsts, int offset, int length) {
            return 0;
        }

        @Override
        public int write(ByteBuffer src) {
            return 0;
        }

        @Override
        public long write(ByteBuffer[] srcs, int offset, int length) {
            return 0;
        }

        @Override
        public long position() {
            return 0;
        }

        @Override
        public FileChannel position(long newPosition) {
            return null;
        }

        @Override
        public long size() {
            return 0;
        }

        @Override
        public FileChannel truncate(long size) {
            return null;
        }

        @Override
        public void force(boolean metaData) {

        }

        @Override
        public long transferTo(long position, long count, WritableByteChannel target) {
            return 0;
        }

        @Override
        public long transferFrom(ReadableByteChannel src, long position, long count) {
            return 0;
        }

        @Override
        public int read(ByteBuffer dst, long position) {
            return 0;
        }

        @Override
        public int write(ByteBuffer src, long position) {
            return 0;
        }

        @Override
        public MappedByteBuffer map(MapMode mode, long position, long size) {
            return null;
        }

        @Override
        public FileLock lock(long position, long size, boolean shared) {
            return null;
        }

        @Override
        public FileLock tryLock(long position, long size, boolean shared) {
            return null;
        }
    }

    static class DummyAsynchronousFileChannel extends AsynchronousFileChannel {
        @Override
        public boolean isOpen() {
            return false;
        }

        @Override
        public void close() {

        }

        @Override
        public long size() {
            return 0;
        }

        @Override
        public AsynchronousFileChannel truncate(long size) {
            return null;
        }

        @Override
        public void force(boolean metaData) {

        }

        @Override
        public <A> void lock(long position, long size, boolean shared, A attachment, CompletionHandler<FileLock, ? super A> handler) {

        }

        @Override
        public Future<FileLock> lock(long position, long size, boolean shared) {
            return null;
        }

        @Override
        public FileLock tryLock(long position, long size, boolean shared) {
            return null;
        }

        @Override
        public <A> void read(ByteBuffer dst, long position, A attachment, CompletionHandler<Integer, ? super A> handler) {

        }

        @Override
        public Future<Integer> read(ByteBuffer dst, long position) {
            return null;
        }

        @Override
        public <A> void write(ByteBuffer src, long position, A attachment, CompletionHandler<Integer, ? super A> handler) {

        }

        @Override
        public Future<Integer> write(ByteBuffer src, long position) {
            return null;
        }
    }

    @SuppressForbidden(reason = "specifically testing readWriteSelectableChannel")
    static class DummySelectableChannelCloser implements Channels.SelectableChannelCloser {
        @Override
        public void implCloseChannel(SelectableChannel sc) {}

        @Override
        public void implReleaseChannel(SelectableChannel sc) {}
    }

    static class DummySelectableChannel extends AbstractSelectableChannel {
        protected DummySelectableChannel(SelectorProvider provider) {
            super(provider);
        }

        @Override
        protected void implCloseSelectableChannel() throws IOException {

        }

        @Override
        protected void implConfigureBlocking(boolean block) throws IOException {

        }

        @Override
        public int validOps() {
            return SelectionKey.OP_ACCEPT | SelectionKey.OP_CONNECT;
        }
    }

    static class DummySelector extends AbstractSelector {
        protected DummySelector(SelectorProvider provider) {
            super(provider);
        }

        @Override
        protected void implCloseSelector() throws IOException {

        }

        @Override
        protected SelectionKey register(AbstractSelectableChannel ch, int ops, Object att) {
            return null;
        }

        @Override
        public Set<SelectionKey> keys() {
            return Set.of();
        }

        @Override
        public Set<SelectionKey> selectedKeys() {
            return Set.of();
        }

        @Override
        public int selectNow() throws IOException {
            return 0;
        }

        @Override
        public int select(long timeout) throws IOException {
            return 0;
        }

        @Override
        public int select() throws IOException {
            return 0;
        }

        @Override
        public Selector wakeup() {
            return null;
        }
    }
}

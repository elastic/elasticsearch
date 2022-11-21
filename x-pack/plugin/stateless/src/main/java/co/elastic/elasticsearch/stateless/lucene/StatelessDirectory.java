package co.elastic.elasticsearch.stateless.lucene;

import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

/**
 * {@link StatelessDirectory} is a {@link FilterDirectory} that allows to register listeners which will be notified of every operation on
 * Lucene files.
 */
public class StatelessDirectory extends FilterDirectory {

    interface Listener {

        default void onRead(String name, IOContext context) {}

        default void onChecksumRead(String name, IOContext context) {}

        default void onWrite(String name, IOContext context) {}

        default void onTempWrite(String name, IOContext context) {}

        default void onSync(Collection<String> names) {}

        default void onSyncMetaData() {};

        default void onRename(String source, String dest) {}

        default void onDelete(String name) {}
    }

    private final List<Listener> listeners = new CopyOnWriteArrayList<>();

    public StatelessDirectory(Directory in) {
        super(Objects.requireNonNull(in));
    }

    /**
     * Registers a new {@link Listener}
     */
    public void addListener(Listener listener) {
        var added = this.listeners.add(listener);
        assert added : "listener already registered: " + listener;
    }

    private void notifyListeners(Consumer<Listener> notify) {
        listeners.forEach(notify);
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        final IndexInput input = super.openInput(name, context);
        notifyListeners(listener -> listener.onRead(name, context));
        return input;
    }

    @Override
    public ChecksumIndexInput openChecksumInput(String name, IOContext context) throws IOException {
        final ChecksumIndexInput input = super.openChecksumInput(name, context);
        notifyListeners(listener -> listener.onChecksumRead(name, context));
        return input;
    }

    @Override
    public IndexOutput createOutput(String name, IOContext context) throws IOException {
        final IndexOutput output = super.createOutput(name, context);
        notifyListeners(listener -> listener.onWrite(name, context));
        return output;
    }

    @Override
    public IndexOutput createTempOutput(String prefix, String suffix, IOContext context) throws IOException {
        final IndexOutput output = super.createTempOutput(prefix, suffix, context);
        notifyListeners(listener -> listener.onTempWrite(output.getName(), context));
        return output;
    }

    @Override
    public void sync(Collection<String> names) throws IOException {
        super.sync(names);
        notifyListeners(listener -> listener.onSync(names));
    }

    @Override
    public void syncMetaData() throws IOException {
        super.syncMetaData();
        notifyListeners(Listener::onSyncMetaData);
    }

    @Override
    public void rename(String source, String dest) throws IOException {
        super.rename(source, dest);
        notifyListeners(listener -> listener.onRename(source, dest));
    }

    @Override
    public void deleteFile(String name) throws IOException {
        super.deleteFile(name);
        notifyListeners(listener -> listener.onDelete(name));
    }

    public static StatelessDirectory unwrapDirectory(final Directory directory) {
        Directory dir = directory;
        while (dir != null) {
            if (dir instanceof StatelessDirectory) {
                return (StatelessDirectory) dir;
            } else if (dir instanceof FilterDirectory) {
                dir = ((FilterDirectory) dir).getDelegate();
            } else {
                dir = null;
            }
        }
        var e = new IllegalStateException(directory.getClass() + " cannot be unwrapped as " + StatelessDirectory.class);
        assert false : e;
        throw e;
    }
}

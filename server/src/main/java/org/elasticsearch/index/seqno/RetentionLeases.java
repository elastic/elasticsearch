package org.elasticsearch.index.seqno;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Objects;

public class RetentionLeases implements Writeable {

    private final long version;

    public long version() {
        return version;
    }

    private final Collection<RetentionLease> retentionLeases;

    public Collection<RetentionLease> retentionLeases() {
        return retentionLeases;
    }

    public static RetentionLeases EMPTY = new RetentionLeases(0, Collections.emptyList());

    public RetentionLeases(final long version, final Collection<RetentionLease> retentionLeases) {
        if (version < 0) {
            throw new IllegalArgumentException("version must be non-negative but was [" + version + "]");
        }
        Objects.requireNonNull(retentionLeases);
        this.version = version;
        this.retentionLeases = Collections.unmodifiableCollection(new ArrayList<>(retentionLeases));
    }

    public RetentionLeases(final StreamInput in) throws IOException {
        version = in.readVLong();
        retentionLeases = in.readList(RetentionLease::new);
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        out.writeVLong(version);
        out.writeCollection(retentionLeases);
    }

}

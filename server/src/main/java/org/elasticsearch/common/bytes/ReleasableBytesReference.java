package org.elasticsearch.common.bytes;

import org.elasticsearch.common.lease.Releasable;

public class ReleasableBytesReference implements Releasable {

    private final BytesReference reference;
    private final Releasable releasable;

    public ReleasableBytesReference(BytesReference reference, Releasable releasable) {
        this.reference = reference;
        this.releasable = releasable;
    }

    public BytesReference getReference() {
        return reference;
    }

    public Releasable getReleasable() {
        return releasable;
    }

    @Override
    public void close() {
        releasable.close();
    }
}

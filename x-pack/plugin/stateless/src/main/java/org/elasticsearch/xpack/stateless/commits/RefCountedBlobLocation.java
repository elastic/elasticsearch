/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless.commits;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.core.AbstractRefCounted;

import java.util.Objects;

public class RefCountedBlobLocation extends AbstractRefCounted {

    private static final Logger logger = LogManager.getLogger(RefCountedBlobLocation.class);

    private final BlobLocation blobLocation;
    private final SubscribableListener<Void> subscribableListener;

    public RefCountedBlobLocation(BlobLocation location) {
        this.blobLocation = Objects.requireNonNull(location);
        this.subscribableListener = new SubscribableListener<>();
    }

    public BlobLocation getBlobLocation() {
        return blobLocation;
    }

    public void addCloseListener(ActionListener<Void> listener) {
        subscribableListener.addListener(listener);
    }

    @Override
    protected void closeInternal() {
        subscribableListener.onResponse(null);
    }

    @Override
    public String toString() {
        return super.toString() + " at " + blobLocation;
    }
}

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

package co.elastic.elasticsearch.stateless.lucene;

import org.apache.lucene.store.IOContext;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.util.Collection;
import java.util.Objects;
import java.util.function.LongSupplier;

public class DefaultDirectoryListener implements StatelessDirectory.Listener {

    private final Logger logger = LogManager.getLogger(DefaultDirectoryListener.class);

    private final ShardId shardId;
    private final LongSupplier primaryTerm;

    public DefaultDirectoryListener(ShardId shardId, LongSupplier primaryTerm) {
        this.shardId = Objects.requireNonNull(shardId);
        this.primaryTerm = Objects.requireNonNull(primaryTerm);
    }

    public void onRead(String name, IOContext context) {
        logger.trace("{} opening [{}] for [read] with {}", shardId, name, context);
    }

    public void onChecksumRead(String name, IOContext context) {
        logger.trace("{} opening [{}] for [checksum] with {}", shardId, name, context);
    }

    public void onWrite(String name, IOContext context) {
        logger.trace("{} opening [{}] for [write] with primary term [{}] and {}", shardId, name, primaryTerm.getAsLong(), context);
    }

    public void onTempWrite(String name, IOContext context) {
        logger.trace(
            "{} opening [{}] for [temporary write] with primary term [{}] and {}",
            shardId,
            name,
            primaryTerm.getAsLong(),
            context
        );
    }

    public void onSync(Collection<String> names) {
        if (logger.isTraceEnabled()) {
            for (String name : names) {
                logger.trace("{} file [{}] synced with primary term [{}]", shardId, name, primaryTerm.getAsLong());
            }
        }
    }

    @Override
    public void onSyncMetaData() {
        logger.trace("{} directory synced", shardId);
    }

    public void onRename(String source, String dest) {
        logger.trace("{} file [{}] renamed to [{}]", shardId, source, dest);
    }

    public void onDelete(String name) {
        logger.trace("{} file [{}] deleted", shardId, name);
    }
}

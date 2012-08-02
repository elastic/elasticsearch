package org.apache.lucene.index;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.LockObtainFailedException;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.index.cache.bloom.BloomCache;

import java.io.IOException;

/**
 */
public class XIndexWriter extends IndexWriter {

    private final ESLogger logger;

    public XIndexWriter(Directory d, IndexWriterConfig conf, ESLogger logger, BloomCache bloomCache) throws CorruptIndexException, LockObtainFailedException, IOException {
        super(d, conf);
        this.logger = logger;
        if (bufferedDeletesStream instanceof XBufferedDeletesStream) {
            logger.debug("using bloom filter enhanced delete handling");
            ((XBufferedDeletesStream) bufferedDeletesStream).setBloomCache(bloomCache);
        }
    }

    public static interface XBufferedDeletesStream {

        void setBloomCache(BloomCache bloomCache);
    }
}

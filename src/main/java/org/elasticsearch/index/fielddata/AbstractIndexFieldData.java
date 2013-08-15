package org.elasticsearch.index.fielddata;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReader;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.settings.IndexSettings;

import java.util.concurrent.atomic.AtomicLong;

/**
 */
public abstract class AbstractIndexFieldData<FD extends AtomicFieldData> extends AbstractIndexComponent implements IndexFieldData<FD> {

    private final FieldMapper.Names fieldNames;
    private final AtomicLong highestUniqueValuesCount = new AtomicLong();
    protected final FieldDataType fieldDataType;
    protected final IndexFieldDataCache cache;

    public AbstractIndexFieldData(Index index, @IndexSettings Settings indexSettings, FieldMapper.Names fieldNames, FieldDataType fieldDataType, IndexFieldDataCache cache) {
        super(index, indexSettings);
        this.fieldNames = fieldNames;
        this.fieldDataType = fieldDataType;
        this.cache = cache;
    }

    @Override
    public FieldMapper.Names getFieldNames() {
        return this.fieldNames;
    }

    @Override
    public void clear() {
        cache.clear(fieldNames.indexName());
    }

    @Override
    public void clear(IndexReader reader) {
        cache.clear(reader);
    }

    @Override
    public long getHighestNumberOfSeenUniqueValues() {
        return highestUniqueValuesCount.get();
    }

    @Override
    public final FD load(AtomicReaderContext context) {
        try {
            FD fd = cache.load(context, this);
            updateHighestSeenValuesCount(fd.getNumberUniqueValues());
            return fd;
        } catch (Throwable e) {
            if (e instanceof ElasticSearchException) {
                throw (ElasticSearchException) e;
            } else {
                throw new ElasticSearchException(e.getMessage(), e);
            }
        }
    }

    private void updateHighestSeenValuesCount(long newValuesCount) {
        long current;
        do {
            if ((current = highestUniqueValuesCount.get()) >= newValuesCount) {
                break;
            }
        } while (!highestUniqueValuesCount.compareAndSet(current, newValuesCount));
    }

}

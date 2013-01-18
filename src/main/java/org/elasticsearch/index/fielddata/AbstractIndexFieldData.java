package org.elasticsearch.index.fielddata;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.settings.IndexSettings;

/**
 */
public abstract class AbstractIndexFieldData<FD extends AtomicFieldData> extends AbstractIndexComponent implements IndexFieldData<FD> {

    private final FieldMapper.Names fieldNames;
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
        cache.clear(index, fieldNames.indexName());
    }
}

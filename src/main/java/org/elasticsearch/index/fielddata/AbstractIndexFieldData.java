package org.elasticsearch.index.fielddata;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.settings.IndexSettings;

/**
 */
public abstract class AbstractIndexFieldData<FD extends AtomicFieldData> extends AbstractIndexComponent implements IndexFieldData<FD> {

    private final String fieldName;
    protected final FieldDataType fieldDataType;
    protected final IndexFieldDataCache cache;

    public AbstractIndexFieldData(Index index, @IndexSettings Settings indexSettings, String fieldName, FieldDataType fieldDataType, IndexFieldDataCache cache) {
        super(index, indexSettings);
        this.fieldName = fieldName;
        this.fieldDataType = fieldDataType;
        this.cache = cache;
    }

    @Override
    public String getFieldName() {
        return this.fieldName;
    }

    @Override
    public void clear() {
        cache.clear(index, fieldName);
    }
}

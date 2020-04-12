package org.elasticsearch.xpack.ql.index;

import java.util.Map;

public class QlFieldCapabilities {
    private final String[] indices;
    private Map<String, Map<String, QlFieldCapability>> fields;
    
    public QlFieldCapabilities(String[] indices) {
        this.indices = indices;
    }

    public Map<String, Map<String, QlFieldCapability>> getFields() {
        return fields;
    }

    public void setFields(Map<String, Map<String, QlFieldCapability>> fields) {
        this.fields = fields;
    }

    public String[] getIndices() {
        return indices;
    }
}

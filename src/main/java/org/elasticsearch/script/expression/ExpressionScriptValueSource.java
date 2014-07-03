package org.elasticsearch.script.expression;


import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.elasticsearch.index.fielddata.AtomicFieldData;
import org.elasticsearch.index.fielddata.AtomicNumericFieldData;
import org.elasticsearch.index.fielddata.IndexFieldData;

import java.io.IOException;
import java.util.Map;

class ExpressionScriptValueSource extends ValueSource {

    IndexFieldData<?> fieldData;

    ExpressionScriptValueSource(IndexFieldData<?> d) {
        fieldData = d;
    }

    @Override
    public FunctionValues getValues(Map context, AtomicReaderContext leaf) throws IOException {
        AtomicFieldData leafData = fieldData.load(leaf);
        assert(leafData instanceof AtomicNumericFieldData);
        return new ExpressionScriptFunctionValues(this, (AtomicNumericFieldData)leafData);
    }

    @Override
    public boolean equals(Object other) {
        return fieldData.equals(other);
    }

    @Override
    public int hashCode() {
        return fieldData.hashCode();
    }

    @Override
    public String description() {
        return "field(" + fieldData.getFieldNames().toString() + ")";
    }
}

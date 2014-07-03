package org.elasticsearch.script.expression;

import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.docvalues.DoubleDocValues;
import org.elasticsearch.index.fielddata.AtomicNumericFieldData;
import org.elasticsearch.index.fielddata.DoubleValues;


class ExpressionScriptFunctionValues extends DoubleDocValues {
    DoubleValues dataAccessor;

    ExpressionScriptFunctionValues(ValueSource parent, AtomicNumericFieldData d) {
        super(parent);
        dataAccessor = d.getDoubleValues();
    }

    @Override
    public double doubleVal(int i) {
        dataAccessor.setDocument(i);
        // TODO: how are default values handled (if doc doesn't have value for this field?)
        // TODO: how to handle nth value for array access in the future?
        return dataAccessor.nextValue();
    }
}

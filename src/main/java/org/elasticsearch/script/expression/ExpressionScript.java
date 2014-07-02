package org.elasticsearch.script.expression;

import org.apache.lucene.expressions.Bindings;
import org.apache.lucene.expressions.Expression;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.search.Scorer;
import org.elasticsearch.script.SearchScript;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 *
 */
class ExpressionScript implements SearchScript {

    Expression expression;
    Bindings bindings;
    AtomicReaderContext leaf;
    int docid;

    ExpressionScript(Expression e, Bindings b) {
        expression = e;
        bindings = b;
    }

    double evaluate() {
        try {
            ValueSource vs = expression.getValueSource(bindings);
            FunctionValues fv = vs.getValues(new HashMap<String, Object>(), leaf);
            return fv.doubleVal(docid);
        } catch (IOException e) {
            throw new ExpressionScriptExecutionException("Failed to run expression", e);
        }
    }

    @Override
    public Object run() { return new Double(evaluate()); }

    @Override
    public float runAsFloat() { return (float)evaluate();}

    @Override
    public long runAsLong() { return (long)evaluate(); }

    @Override
    public double runAsDouble() { return evaluate(); }

    @Override
    public Object unwrap(Object value) { return value; }

    @Override
    public void setNextDocId(int d) {
        docid = d;
    }

    @Override
    public void setNextReader(AtomicReaderContext l) {
        leaf = l;
    }

    @Override
    public void setNextSource(Map<String, Object> source) {

    }

    @Override
    public void setNextScore(float score) {

    }


    @Override
    public void setNextVar(String name, Object value) {

    }



    @Override
    public void setScorer(Scorer scorer) {

    }
}

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.script.expression;

import org.apache.lucene.expressions.Expression;
import org.apache.lucene.expressions.SimpleBindings;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DoubleValues;
import org.apache.lucene.search.DoubleValuesSource;
import org.elasticsearch.script.FieldScript;
import org.elasticsearch.script.GeneralScriptException;

import java.io.IOException;

public class ExpressionFieldScript implements FieldScript.LeafFactory {
    private final Expression exprScript;
    private final DoubleValuesSource source;

    ExpressionFieldScript(Expression e, SimpleBindings b) {
        this.exprScript = e;
        this.source = exprScript.getDoubleValuesSource(b);
    }

    @Override
    public FieldScript newInstance(final LeafReaderContext leaf) throws IOException {
        return new FieldScript() {

            // Fake the scorer until setScorer is called.
            DoubleValues values = source.getValues(leaf, null);

            @Override
            public Object execute() {
                try {
                    return values.doubleValue();
                } catch (Exception exception) {
                    throw new GeneralScriptException("Error evaluating " + exprScript, exception);
                }
            }

            @Override
            public void setDocument(int d) {
                try {
                    values.advanceExact(d);
                } catch (IOException e) {
                    throw new IllegalStateException("Can't advance to doc using " + exprScript, e);
                }
            }
        };
    }
}

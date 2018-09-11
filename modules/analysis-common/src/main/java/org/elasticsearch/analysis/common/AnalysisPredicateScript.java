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

package org.elasticsearch.analysis.common;

import org.elasticsearch.script.ScriptContext;

/**
 * A predicate based on the current token in a TokenStream
 */
public abstract class AnalysisPredicateScript {

    /**
     * Encapsulation of the state of the current token
     */
    public static class Token {
        public CharSequence term;
        public int pos;
        public int posInc;
        public int posLen;
        public int startOffset;
        public int endOffset;
        public String type;
        public boolean isKeyword;

        public CharSequence getTerm() {
            return term;
        }

        public int getPositionIncrement() {
            return posInc;
        }

        public int getPosition() {
            return pos;
        }

        public int getPositionLength() {
            return posLen;
        }

        public int getStartOffset() {
            return startOffset;
        }

        public int getEndOffset() {
            return endOffset;
        }

        public String getType() {
            return type;
        }

        public boolean isKeyword() {
            return isKeyword;
        }
    }

    /**
     * Returns {@code true} if the current term matches the predicate
     */
    public abstract boolean execute(Token token);

    public interface Factory {
        AnalysisPredicateScript newInstance();
    }

    public static final String[] PARAMETERS = new String[]{ "token" };
    public static final ScriptContext<Factory> CONTEXT = new ScriptContext<>("analysis", Factory.class);

}

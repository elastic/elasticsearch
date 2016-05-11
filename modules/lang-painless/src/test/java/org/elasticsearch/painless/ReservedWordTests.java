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

package org.elasticsearch.painless;

/** Tests for special reserved words such as _score */
public class ReservedWordTests extends ScriptTestCase {
    
    /** check that we can't declare a variable of _score, its really reserved! */
    public void testScoreVar() {
        IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
            exec("int _score = 5; return _score;");
        });
        assertTrue(expected.getMessage().contains("Variable name [_score] already defined"));
    }
    
    /** check that we can't write to _score, its read-only! */
    public void testScoreStore() {
        IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
            exec("_score = 5; return _score;");
        });
        assertTrue(expected.getMessage().contains("Variable [_score] is read-only"));
    }
    
    /** check that we can't declare a variable of doc, its really reserved! */
    public void testDocVar() {
        IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
            exec("int doc = 5; return doc;");
        });
        assertTrue(expected.getMessage().contains("Variable name [doc] already defined"));
    }
    
    /** check that we can't write to _score, its read-only! */
    public void testDocStore() {
        IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> {
            exec("doc = 5; return doc;");
        });
        assertTrue(expected.getMessage().contains("Variable [doc] is read-only"));
    }
}

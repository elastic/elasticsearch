/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.painless;

public class ConstantFoldingTests extends ScriptTestCase {

    public void testUnary() {
        assertBytecodeExists("~1", "BIPUSH -2");
        assertBytecodeExists("~1000L", "LDC -1001");
        assertBytecodeExists("!true", "ICONST_0");
    }

    public void testBinary() {
        assertBytecodeExists("2*2", "ICONST_4");
        assertBytecodeExists("2L*2L", "LDC 4");
        assertBytecodeExists("2.0F*2.0F", "LDC 4.0");
        assertBytecodeExists("2.0*2.0", "LDC 4.0");
        assertBytecodeExists("2/2", "ICONST_1");
        assertBytecodeExists("2L/2L", "LCONST_1");
        assertBytecodeExists("2.0F/2.0F", "FCONST_1");
        assertBytecodeExists("2.0/2.0", "DCONST_1");
        assertBytecodeExists("2%2", "ICONST_0");
        assertBytecodeExists("2L%2L", "LCONST_0");
        assertBytecodeExists("2.0F%2.0F", "FCONST_0");
        assertBytecodeExists("2.0%2.0", "DCONST_0");
        assertBytecodeExists("2+3", "ICONST_5");
        assertBytecodeExists("2L+3L", "LDC 5");
        assertBytecodeExists("2.0F+3.0F", "LDC 5.0");
        assertBytecodeExists("2.0+3.0", "LDC 5.0");
        assertBytecodeExists("2-3", "ICONST_M1");
        assertBytecodeExists("2L-3L", "LDC -1");
        assertBytecodeExists("2.0F-3.0F", "LDC -1.0");
        assertBytecodeExists("2.0-3.0", "LDC -1.0");
        assertBytecodeExists("2<<1", "ICONST_4");
        assertBytecodeExists("2L<<1L", "LDC 4");
        assertBytecodeExists("4>>1", "ICONST_2");
        assertBytecodeExists("4L>>1L", "LDC 2");
        assertBytecodeExists("4>>>1", "ICONST_2");
        assertBytecodeExists("4L>>>1L", "LDC 2");
        assertBytecodeExists("3&1", "ICONST_1");
        assertBytecodeExists("3L&1L", "LDC 1");
        assertBytecodeExists("true^false", "ICONST_1");
        assertBytecodeExists("3^1", "ICONST_2");
        assertBytecodeExists("3L^1L", "LDC 2");
        assertBytecodeExists("3|1", "ICONST_3");
        assertBytecodeExists("3L|1L", "LDC 3");
    }

    public void testStringConcatenation() {
        assertBytecodeExists("'x' + 'y' + 'z'", "LDC \"xyz\"");
    }

    public void testBoolean() {
        assertBytecodeExists("true && false", "ICONST_0");
        assertBytecodeExists("true || false", "ICONST_1");
    }

    public void testComparison() {
        assertBytecodeExists("2==2", "ICONST_1");
        assertBytecodeExists("2L==2L", "ICONST_1");
        assertBytecodeExists("2.0F==2.0F", "ICONST_1");
        assertBytecodeExists("2.0==2.0", "ICONST_1");
        assertBytecodeExists("'x'=='x'", "ICONST_1");
        assertBytecodeExists("2!=2", "ICONST_0");
        assertBytecodeExists("2L!=2L", "ICONST_0");
        assertBytecodeExists("2.0F!=2.0F", "ICONST_0");
        assertBytecodeExists("2.0!=2.0", "ICONST_0");
        assertBytecodeExists("'x'!='x'", "ICONST_0");
        assertBytecodeExists("2===2", "ICONST_1");
        assertBytecodeExists("2L===2L", "ICONST_1");
        assertBytecodeExists("2.0F===2.0F", "ICONST_1");
        assertBytecodeExists("2.0===2.0", "ICONST_1");
        assertBytecodeExists("'x'==='x'", "ICONST_0");
        assertBytecodeExists("2!==2", "ICONST_0");
        assertBytecodeExists("2L!==2L", "ICONST_0");
        assertBytecodeExists("2.0F!==2.0F", "ICONST_0");
        assertBytecodeExists("2.0!==2.0", "ICONST_0");
        assertBytecodeExists("'x'!=='x'", "ICONST_1");
        assertBytecodeExists("2>2", "ICONST_0");
        assertBytecodeExists("2L>2L", "ICONST_0");
        assertBytecodeExists("2.0F>2.0F", "ICONST_0");
        assertBytecodeExists("2.0>2.0", "ICONST_0");
        assertBytecodeExists("2>=2", "ICONST_1");
        assertBytecodeExists("2L>=2L", "ICONST_1");
        assertBytecodeExists("2.0F>=2.0F", "ICONST_1");
        assertBytecodeExists("2.0>=2.0", "ICONST_1");
        assertBytecodeExists("2<2", "ICONST_0");
        assertBytecodeExists("2L<2L", "ICONST_0");
        assertBytecodeExists("2.0F<2.0F", "ICONST_0");
        assertBytecodeExists("2.0<2.0", "ICONST_0");
        assertBytecodeExists("2<=2", "ICONST_1");
        assertBytecodeExists("2L<=2L", "ICONST_1");
        assertBytecodeExists("2.0F<=2.0F", "ICONST_1");
        assertBytecodeExists("2.0<=2.0", "ICONST_1");
    }

    public void testCast() {
        assertBytecodeExists("2==2L", "ICONST_1");
        assertBytecodeExists("2+2D", "LDC 4.0");
        assertBytecodeExists("2+'2D'", "LDC \"22D\"");
        assertBytecodeExists("4L<5F", "ICONST_1");
    }
}

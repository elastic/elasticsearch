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

package org.elasticsearch.test;

/** basic tests */
public class SimpleTests extends BaseTestCase {

    /** drops the exception on the floor */
    public int escapes() {
        try {
            return Integer.parseInt("bogus");
        } catch (Exception e) {
            return 0;
        }
    }
    
    public void testEscapes() throws Exception {
        MethodAnalyzer analyzer = analyze(getClass().getMethod("escapes"));
        assertEquals(1, analyzer.violations.size());
        Violation violation = analyzer.violations.iterator().next();
        assertEquals(Violation.Kind.ESCAPES_WITHOUT_THROWING_ANYTHING, violation.kind);
    }
    
    /** drops the exception on the floor (sometimes) */
    public int escapesSometimes() {
        try {
            return Integer.parseInt("bogus");
        } catch (RuntimeException e) {
            if (e.getMessage().equals("ok")) {
                return 0;
            } else {
                throw e;
            }
        }
    }
    
    public void testEscapesSometimes() throws Exception {
        MethodAnalyzer analyzer = analyze(getClass().getMethod("escapesSometimes"));
        assertEquals(1, analyzer.violations.size());
        Violation violation = analyzer.violations.iterator().next();
        assertEquals(Violation.Kind.ESCAPES_WITHOUT_THROWING_ANYTHING, violation.kind);
    }
    
    /** drops the exception on the floor (sometimes, with loop) */
    public int escapesSometimesLoop() {
        try {
            return Integer.parseInt("bogus");
        } catch (RuntimeException e) {
            while (e != null) {
              throw e;
            }
            return 0;
        }
    }
    
    public void testEscapesSometimesLoop() throws Exception {
        MethodAnalyzer analyzer = analyze(getClass().getMethod("escapesSometimesLoop"));
        assertEquals(1, analyzer.violations.size());
        Violation violation = analyzer.violations.iterator().next();
        assertEquals(Violation.Kind.ESCAPES_WITHOUT_THROWING_ANYTHING, violation.kind);
    }
    
    /** throws something else (does not pass the exception) */
    public int throwsSomethingElse() {
        try {
            return Integer.parseInt("bogus");
        } catch (Exception e) {
            throw new NullPointerException();
        }
    }
    
    public void testThrowsSomethingElse() throws Exception {
        MethodAnalyzer analyzer = analyze(getClass().getMethod("throwsSomethingElse"));
        Violation violation = analyzer.violations.iterator().next();
        assertEquals(Violation.Kind.THROWS_SOMETHING_ELSE_BUT_LOSES_ORIGINAL, violation.kind);
    }
    
    /** throws exception back directly */
    public int throwsExceptionBack() {
        try {
            return Integer.parseInt("bogus");
        } catch (RuntimeException e) {
            throw e;
        }
    }
    
    public void testThrowsExceptionBack() throws Exception {
        MethodAnalyzer analyzer = analyze(getClass().getMethod("throwsExceptionBack"));
        assertTrue(analyzer.violations.isEmpty());
    }
    
    /** throws exception boxed in another */
    public int throwsBoxedException() {
        try {
            return Integer.parseInt("bogus");
        } catch (RuntimeException e) {
            throw new RuntimeException(e);
        }
    }
    
    public void testThrowsBoxedException() throws Exception {
        MethodAnalyzer analyzer = analyze(getClass().getMethod("throwsBoxedException"));
        assertTrue(analyzer.violations.isEmpty());
    }
    
    /** throws exception boxed in another (via initCause) */
    public int throwsBoxedExceptionInitCause() {
        try {
            return Integer.parseInt("bogus");
        } catch (RuntimeException e) {
            RuntimeException f = new RuntimeException();
            f.initCause(e);
            throw f;
        }
    }
    
    public void testThrowsBoxedExceptionInitCause() throws Exception {
        MethodAnalyzer analyzer = analyze(getClass().getMethod("throwsBoxedExceptionInitCause"));
        assertTrue(analyzer.violations.isEmpty());
    }
    
    /** throws exception boxed in another (via addSuppressed) */
    public int throwsBoxedExceptionAddSuppressed() {
        try {
            return Integer.parseInt("bogus");
        } catch (RuntimeException e) {
            RuntimeException f = new RuntimeException();
            f.addSuppressed(e);
            throw f;
        }
    }
    
    public void testThrowsBoxedExceptionAddSuppressed() throws Exception {
        MethodAnalyzer analyzer = analyze(getClass().getMethod("throwsBoxedExceptionAddSuppressed"));
        assertTrue(analyzer.violations.isEmpty());
    }
}

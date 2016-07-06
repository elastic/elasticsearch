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

import java.io.ByteArrayOutputStream;

/** basic tests, using try-with resources */
public class SimpleWithResourcesTests extends BaseTestCase {

    /** drops the exception on the floor */
    public int escapes() {
        try (ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            assert os != null;
            return Integer.parseInt("bogus");
        } catch (Exception e) {
            return 0;
        }
    }
    
    public void testEscapes() throws Exception {
        MethodAnalyzer analyzer = analyze(getClass().getMethod("escapes"));
        assertFalse(analyzer.violations.isEmpty());
        for (Violation violation : analyzer.violations) {
            assertEquals(Violation.Kind.ESCAPES_WITHOUT_THROWING_ANYTHING, violation.kind);
        }
    }
    
    /** drops the exception on the floor (sometimes) */
    public int escapesSometimes() throws Exception {
        try (ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            assert os != null;
            return Integer.parseInt("bogus");
        } catch (Exception e) {
            if (e.getMessage().equals("ok")) {
                return 0;
            } else {
                throw e;
            }
        }
    }
    
    public void testEscapesSometimes() throws Exception {
        MethodAnalyzer analyzer = analyze(getClass().getMethod("escapesSometimes"));
        assertFalse(analyzer.violations.isEmpty());
        for (Violation violation : analyzer.violations) {
            assertEquals(Violation.Kind.ESCAPES_WITHOUT_THROWING_ANYTHING, violation.kind);
        }
    }
    
    /** drops the exception on the floor (sometimes, with loop) */
    public int escapesSometimesLoop() throws Exception {
        try (ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            assert os != null;
            return Integer.parseInt("bogus");
        } catch (Exception e) {
            while (e != null) {
              throw e;
            }
            return 0;
        }
    }
    
    public void testEscapesSometimesLoop() throws Exception {
        MethodAnalyzer analyzer = analyze(getClass().getMethod("escapesSometimesLoop"));
        assertFalse(analyzer.violations.isEmpty());
        for (Violation violation : analyzer.violations) {
            assertEquals(Violation.Kind.ESCAPES_WITHOUT_THROWING_ANYTHING, violation.kind);
        }
    }
    
    /** throws something else (does not pass the exception) */
    public int throwsSomethingElse() {
        try (ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            assert os != null;
            return Integer.parseInt("bogus");
        } catch (Exception e) {
            throw new NullPointerException();
        }
    }
    
    public void testThrowsSomethingElse() throws Exception {
        MethodAnalyzer analyzer = analyze(getClass().getMethod("throwsSomethingElse"));
        assertFalse(analyzer.violations.isEmpty());
        for (Violation violation : analyzer.violations) {
            assertEquals(Violation.Kind.THROWS_SOMETHING_ELSE_BUT_LOSES_ORIGINAL, violation.kind);
        }
    }
    
    /** throws exception back directly */
    public int throwsExceptionBack() throws Exception {
        try (ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            assert os != null;
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
    public int throwsBoxedException() throws Exception {
        try (ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            assert os != null;
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
    public int throwsBoxedExceptionInitCause() throws Exception {
        try (ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            assert os != null;
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
    public int throwsBoxedExceptionAddSuppressed() throws Exception {
        try (ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            assert os != null;
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

/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.groovy.util.xcontent

/**
 * @author kimchy (shay.banon)
 */
class GXContentBuilderTests extends GroovyTestCase {

    void testSimple() {
        def builder = new GXContentBuilder()

        def result = builder.buildAsString {
            rootprop = "something"
        }

        assertEquals '{"rootprop":"something"}', result.toString()
    }

    void testArrays() {
        def builder = new GXContentBuilder()

        def result = builder.buildAsString {
            categories = ['a', 'b', 'c']
            rootprop = "something"
        }

        assertEquals '{"categories":["a","b","c"],"rootprop":"something"}', result.toString()
    }

    void testSubObjects() {
        def builder = new GXContentBuilder()

        def result = builder.buildAsString {
            categories = ['a', 'b', 'c']
            rootprop = "something"
            test {
                subprop = 10
            }
        }

        assertEquals '{"categories":["a","b","c"],"rootprop":"something","test":{"subprop":10}}', result.toString()
    }

    void testAssignedObjects() {
        def builder = new GXContentBuilder()

        def result = builder.buildAsString {
            categories = ['a', 'b', 'c']
            rootprop = "something"
            test = {
                subprop = 10
            }
        }

        assertEquals '{"categories":["a","b","c"],"rootprop":"something","test":{"subprop":10}}', result.toString()
    }

    void testNamedArgumentHandling() {
        def builder = new GXContentBuilder()
        def result = builder.buildAsString {
            categories = ['a', 'b', 'c']
            rootprop = "something"
            test subprop: 10, three: [1, 2, 3]

        }

        assertEquals '{"categories":["a","b","c"],"rootprop":"something","test":{"subprop":10,"three":[1,2,3]}}', result.toString()
    }


    void testArrayOfClosures() {
        def builder = new GXContentBuilder()
        def result = builder.buildAsString {
            foo = [{ bar = "hello" }]
        }

        assertEquals '{"foo":[{"bar":"hello"}]}', result.toString()
    }

    void testRootElementList() {
        def builder = new GXContentBuilder()

        def results = ['one', 'two', 'three']

        def result = builder.buildAsString {
            for (b in results) {
                element b
            }
        }

        assertEquals '["one","two","three"]', result.toString()

        result = builder.buildAsString {
            results
        }

        assertEquals '["one","two","three"]', result.toString()

    }

    void testExampleFromReferenceGuide() {
        def builder = new GXContentBuilder()

        def results = ['one', 'two', 'three']

        def result = builder.buildAsString {
            for (b in results) {
                element title: b
            }
        }

        assertEquals '[{"title":"one"},{"title":"two"},{"title":"three"}]', result.toString()


        result = builder.buildAsString {
            books = results.collect {
                [title: it]
            }
        }

        assertEquals '{"books":[{"title":"one"},{"title":"two"},{"title":"three"}]}', result.toString()

        result = builder.buildAsString {
            books = array {
                for (b in results) {
                    book title: b
                }
            }
        }

        assertEquals '{"books":[{"title":"one"},{"title":"two"},{"title":"three"}]}', result.toString()
    }

    void testAppendToArray() {
        def builder = new GXContentBuilder()

        def results = ['one', 'two', 'three']

        def result = builder.buildAsString {
            books = array {list ->
                for (b in results) {
                    list << [title: b]
                }
            }
        }

        assertEquals '{"books":[{"title":"one"},{"title":"two"},{"title":"three"}]}', result.toString()
    }
}

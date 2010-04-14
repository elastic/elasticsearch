package org.elasticsearch.groovy.util.json

/**
 * @author kimchy (shay.banon)
 */
class JsonBuilderTests extends GroovyTestCase {

    void testSimple() {
        def builder = new JsonBuilder()

        def result = builder.buildAsString {
            rootprop = "something"
        }

        assertEquals '{"rootprop":"something"}', result.toString()
    }

    void testArrays() {
        def builder = new JsonBuilder()

        def result = builder.buildAsString {
            categories = ['a', 'b', 'c']
            rootprop = "something"
        }

        assertEquals '{"categories":["a","b","c"],"rootprop":"something"}', result.toString()
    }

    void testSubObjects() {
        def builder = new JsonBuilder()

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
        def builder = new JsonBuilder()

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
        def builder = new JsonBuilder()
        def result = builder.buildAsString {
            categories = ['a', 'b', 'c']
            rootprop = "something"
            test subprop: 10, three: [1, 2, 3]

        }

        assertEquals '{"categories":["a","b","c"],"rootprop":"something","test":{"subprop":10,"three":[1,2,3]}}', result.toString()
    }


    void testArrayOfClosures() {
        def builder = new JsonBuilder()
        def result = builder.buildAsString {
            foo = [{ bar = "hello" }]
        }

        assertEquals '{"foo":[{"bar":"hello"}]}', result.toString()
    }

    void testRootElementList() {
        def builder = new JsonBuilder()

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
        def builder = new JsonBuilder()

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
        def builder = new JsonBuilder()

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

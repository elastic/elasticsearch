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

package org.elasticsearch.groovy.common.xcontent

import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.common.xcontent.XContentFactory
import org.elasticsearch.common.xcontent.XContentType

/**
 * Used to build JSON data.
 *
 * @author Marc Palmer
 * @author Graeme Rocher
 *
 * @since 1.2
 */
class GXContentBuilder {

    static NODE_ELEMENT = 'element'

    static int rootResolveStrategy = Closure.OWNER_FIRST // the default in Closure

    def root

    def current

    def nestingStack = []

    def build(Closure c) {
        return buildRoot(c)
    }

    String buildAsString(Closure c) {
        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON)
        def json = build(c)
        builder.map(json)
        return builder.string()
    }

    byte[] buildAsBytes(Closure c) {
        return buildAsBytes(c, XContentType.JSON)
    }

    byte[] buildAsBytes(Closure c, XContentType contentType) {
        XContentBuilder builder = XContentFactory.contentBuilder(contentType)
        def json = build(c)
        builder.map(json)
        return builder.copiedBytes()
    }

    private buildRoot(Closure c) {
        c.delegate = this
        c.resolveStrategy = rootResolveStrategy
        root = [:]
        current = root
        def returnValue = c.call()
        if (!root) {
            return returnValue
        }
        return root
    }

    def invokeMethod(String methodName) {
        current[methodName] = []
    }

    List array(Closure c) {
        def prev = current
        def list = []
        try {
            current = list
            c.call(list)
        }
        finally {
            current = prev
        }
        return list
    }

    def invokeMethod(String methodName, Object args) {
        if (args.size()) {
            if (args[0] instanceof Map) {
                // switch root to an array if elements used at top level
                if ((current == root) && (methodName == NODE_ELEMENT) && !(root instanceof List)) {
                    if (root.size()) {
                        throw new IllegalArgumentException('Cannot have array elements in root node if properties of root have already been set')
                    } else {
                        root = []
                        current = root
                    }
                }
                def n = [:]
                if (current instanceof List) {
                    current << n
                } else {
                    current[methodName] = n
                }
                n.putAll(args[0])
            } else if (args[-1] instanceof Closure) {
                final Object callable = args[-1]
                handleClosureNode(methodName, callable)
            } else if (args.size() == 1) {
                if (methodName != NODE_ELEMENT) {
                    throw new IllegalArgumentException('Array elements must be defined with the "element" method call eg: element(value)')
                }
                // switch root to an array if elements used at top level
                if (current == root) {
                    if (root.size() && methodName != NODE_ELEMENT) {
                        throw new IllegalArgumentException('Cannot have array elements in root node if properties of root have already been set')
                    } else if (!(root instanceof List)) {
                        root = []
                        current = root
                    }
                }
                if (current instanceof List) {
                    current << args[0]
                } else {
                    throw new IllegalArgumentException('Array elements can only be defined under "array" nodes')
                }
            } else {
                throw new IllegalArgumentException("This builder does not support invocation of [$methodName] with arg list ${args.dump()}")
            }
        } else {
            current[methodName] = []
        }
    }

    private handleClosureNode(String methodName, callable) {
        def n = [:]
        nestingStack << current

        if (current instanceof List) {
            current << n
        }
        else {
            current[methodName] = n
        }
        current = n
        callable.call()
        current = nestingStack.pop()
    }


    void setProperty(String propName, Object value) {
        if (value instanceof Closure) {
            handleClosureNode(propName, value)
        }
        else if (value instanceof List) {
            value = value.collect {
                if (it instanceof Closure) {
                    def callable = it
                    final GXContentBuilder localBuilder = new GXContentBuilder()
                    callable.delegate = localBuilder
                    callable.resolveStrategy = Closure.DELEGATE_FIRST
                    final Map nestedObject = localBuilder.buildRoot(callable)
                    return nestedObject
                }
                else {
                    return it
                }
            }
            current[propName] = value
        }
        else {
            current[propName] = value
        }
    }

    def getProperty(String propName) {
        current[propName]
    }
}

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

package org.elasticsearch.painless.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;

import java.util.ArrayList;
import java.util.List;

public class ContextInfoTests extends AbstractSerializingTestCase<PainlessContextInfo> {

    @Override
    protected PainlessContextInfo doParseInstance(XContentParser parser) {
        return PainlessContextInfo.fromXContent(parser);
    }

    @Override
    protected PainlessContextInfo createTestInstance() {
        int classesSize = randomIntBetween(20, 100);
        List<PainlessContextClassInfo> classes = new ArrayList<>();

        for (int clazz = 0; clazz < classesSize; ++clazz) {
            int constructorsSize = randomInt(4);
            List<PainlessContextConstructorInfo> constructors = new ArrayList<>(constructorsSize);
            for (int constructor = 0; constructor < constructorsSize; ++constructor) {
                int parameterSize = randomInt(12);
                List<String> parameters = new ArrayList<>(parameterSize);
                for (int parameter = 0; parameter < parameterSize; ++parameter) {
                    parameters.add(randomAlphaOfLengthBetween(1, 20));
                }
                constructors.add(new PainlessContextConstructorInfo(
                        randomAlphaOfLength(randomIntBetween(4, 10)),
                        parameters));
            }
            ;

            int staticMethodsSize = randomInt(4);
            List<PainlessContextMethodInfo> staticMethods = new ArrayList<>(staticMethodsSize);
            for (int staticMethod = 0; staticMethod < staticMethodsSize; ++staticMethod) {
                int parameterSize = randomInt(12);
                List<String> parameters = new ArrayList<>(parameterSize);
                for (int parameter = 0; parameter < parameterSize; ++parameter) {
                    parameters.add(randomAlphaOfLengthBetween(1, 20));
                }
                staticMethods.add(new PainlessContextMethodInfo(
                        randomAlphaOfLength(randomIntBetween(4, 10)),
                        randomAlphaOfLength(randomIntBetween(4, 10)),
                        randomAlphaOfLength(randomIntBetween(4, 10)),
                        parameters));
            }

            int methodsSize = randomInt(10);
            List<PainlessContextMethodInfo> methods = new ArrayList<>(methodsSize);
            for (int method = 0; method < methodsSize; ++method) {
                int parameterSize = randomInt(12);
                List<String> parameters = new ArrayList<>(parameterSize);
                for (int parameter = 0; parameter < parameterSize; ++parameter) {
                    parameters.add(randomAlphaOfLengthBetween(1, 20));
                }
                methods.add(new PainlessContextMethodInfo(
                        randomAlphaOfLength(randomIntBetween(4, 10)),
                        randomAlphaOfLength(randomIntBetween(4, 10)),
                        randomAlphaOfLength(randomIntBetween(4, 10)),
                        parameters));
            }

            int staticFieldsSize = randomInt(10);
            List<PainlessContextFieldInfo> staticFields = new ArrayList<>();
            for (int staticField = 0; staticField < staticFieldsSize; ++staticField) {
                staticFields.add(new PainlessContextFieldInfo(
                        randomAlphaOfLength(randomIntBetween(4, 10)),
                        randomAlphaOfLength(randomIntBetween(4, 10)),
                        randomAlphaOfLength(randomIntBetween(4, 10))));
            }

            int fieldsSize = randomInt(4);
            List<PainlessContextFieldInfo> fields = new ArrayList<>();
            for (int field = 0; field < fieldsSize; ++field) {
                fields.add(new PainlessContextFieldInfo(
                        randomAlphaOfLength(randomIntBetween(4, 10)),
                        randomAlphaOfLength(randomIntBetween(4, 10)),
                        randomAlphaOfLength(randomIntBetween(4, 10))));
            }

            classes.add(new PainlessContextClassInfo(
                    randomAlphaOfLength(randomIntBetween(3, 200)), randomBoolean(),
                    constructors, staticMethods, methods, fields, staticFields));
        }

        int importedMethodsSize = randomInt(4);
        List<PainlessContextMethodInfo> importedMethods = new ArrayList<>(importedMethodsSize);
        for (int importedMethod = 0; importedMethod < importedMethodsSize; ++importedMethod) {
            int parameterSize = randomInt(12);
            List<String> parameters = new ArrayList<>(parameterSize);
            for (int parameter = 0; parameter < parameterSize; ++parameter) {
                parameters.add(randomAlphaOfLengthBetween(1, 20));
            }
            importedMethods.add(new PainlessContextMethodInfo(
                    randomAlphaOfLength(randomIntBetween(4, 10)),
                    randomAlphaOfLength(randomIntBetween(4, 10)),
                    randomAlphaOfLength(randomIntBetween(4, 10)),
                    parameters));
        }
        
        int classBindingsSize = randomInt(3);
        List<PainlessContextClassBindingInfo> classBindings = new ArrayList<>(classBindingsSize);
        for (int classBinding = 0; classBinding < classBindingsSize; ++classBinding) {
            int parameterSize = randomIntBetween(2, 5);
            int readOnly = randomIntBetween(1, parameterSize - 1);
            List<String> parameters = new ArrayList<>(parameterSize);
            for (int parameter = 0; parameter < parameterSize; ++parameter) {
                parameters.add(randomAlphaOfLengthBetween(1, 20));
            }
            classBindings.add(new PainlessContextClassBindingInfo(
                    randomAlphaOfLength(randomIntBetween(4, 10)),
                    randomAlphaOfLength(randomIntBetween(4, 10)),
                    randomAlphaOfLength(randomIntBetween(4, 10)),
                    readOnly,
                    parameters));
        }

        int instanceBindingsSize = randomInt(3);
        List<PainlessContextInstanceBindingInfo> instanceBindings = new ArrayList<>(classBindingsSize);
        for (int instanceBinding = 0; instanceBinding < instanceBindingsSize; ++instanceBinding) {
            int parameterSize = randomInt(12);
            List<String> parameters = new ArrayList<>(parameterSize);
            for (int parameter = 0; parameter < parameterSize; ++parameter) {
                parameters.add(randomAlphaOfLengthBetween(1, 20));
            }
            instanceBindings.add(new PainlessContextInstanceBindingInfo(
                    randomAlphaOfLength(randomIntBetween(4, 10)),
                    randomAlphaOfLength(randomIntBetween(4, 10)),
                    randomAlphaOfLength(randomIntBetween(4, 10)),
                    parameters));
        }
        
        return new PainlessContextInfo(randomAlphaOfLength(20),
                classes, importedMethods, classBindings, instanceBindings);
    }

    @Override
    protected Writeable.Reader<PainlessContextInfo> instanceReader() {
        return PainlessContextInfo::new;
    }
}

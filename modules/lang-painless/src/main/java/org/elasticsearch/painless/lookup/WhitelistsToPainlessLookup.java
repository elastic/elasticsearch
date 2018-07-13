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

package org.elasticsearch.painless.lookup;

import org.elasticsearch.painless.spi.Whitelist;
import org.elasticsearch.painless.spi.WhitelistClass;
import org.elasticsearch.painless.spi.WhitelistConstructor;
import org.elasticsearch.painless.spi.WhitelistField;
import org.elasticsearch.painless.spi.WhitelistMethod;

import java.util.List;

public class WhitelistsToPainlessLookup {
    public static PainlessLookup build(List<Whitelist> whitelists) {
        PainlessLookupBuilder painlessLookupBuilder = new PainlessLookupBuilder();
        String origin = null;

        try {
            for (Whitelist whitelist : whitelists) {
                for (WhitelistClass whitelistClass : whitelist.whitelistStructs) {
                    origin = whitelistClass.origin;
                    painlessLookupBuilder.addPainlessClass(
                            whitelist.javaClassLoader, whitelistClass.javaClassName, whitelistClass.onlyFQNJavaClassName == false);
                }
            }

            for (Whitelist whitelist : whitelists) {
                for (WhitelistClass whitelistClass : whitelist.whitelistStructs) {
                    for (WhitelistConstructor whitelistConstructor : whitelistClass.whitelistConstructors) {
                        origin = whitelistConstructor.origin;
                        painlessLookupBuilder.addPainlessConstructor(
                                whitelistClass.javaClassName, whitelistConstructor.painlessParameterTypeNames);
                    }

                    for (WhitelistMethod whitelistMethod : whitelistClass.whitelistMethods) {
                        origin = whitelistMethod.origin;
                        painlessLookupBuilder.addPainlessMethod(
                                whitelist.javaClassLoader, whitelistClass.javaClassName, whitelistMethod.javaAugmentedClassName,
                                whitelistMethod.javaMethodName, whitelistMethod.painlessReturnTypeName,
                                whitelistMethod.painlessParameterTypeNames);
                    }

                    for (WhitelistField whitelistField : whitelistClass.whitelistFields) {
                        origin = whitelistField.origin;
                        painlessLookupBuilder.addPainlessField(
                                whitelistClass.javaClassName, whitelistField.javaFieldName, whitelistField.painlessFieldTypeName);
                    }
                }
            }
        } catch (Exception exception) {
            throw new IllegalArgumentException("error loading whitelist(s) " + origin, exception);
        }

        return painlessLookupBuilder.build();
    }

    private WhitelistsToPainlessLookup() {

    }
}

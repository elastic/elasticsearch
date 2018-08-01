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

import java.lang.invoke.MethodType;

public class FunctionReference {

    /** functional interface method name */
    public final String interfaceMethodName;
    /** functional interface method signature */
    public final MethodType interfaceMethodType;
    /** class of the delegate method to be called */
    public final String delegateClassName;
    /** whether a call is made on a delegate interface */
    public final boolean isDelegateInterface;
    /** the invocation type of the delegate method */
    public final int delegateInvokeType;
    /** the name of the delegate method */
    public final String delegateMethodName;
    /** delegate method signature */
    public final MethodType delegateMethodType;
    /** factory (CallSite) method signature */
    public final MethodType factoryMethodType;

    public FunctionReference(
            String interfaceMethodName, MethodType interfaceMethodType,
            String delegateClassName, boolean isDelegateInterface,
            int delegateInvokeType, String delegateMethodName, MethodType delegateMethodType,
            MethodType factoryMethodType) {

        this.interfaceMethodName = interfaceMethodName;
        this.interfaceMethodType = interfaceMethodType;
        this.delegateClassName = delegateClassName;
        this.isDelegateInterface = isDelegateInterface;
        this.delegateInvokeType = delegateInvokeType;
        this.delegateMethodName = delegateMethodName;
        this.delegateMethodType = delegateMethodType;
        this.factoryMethodType = factoryMethodType;
    }
}

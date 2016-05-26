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

package org.elasticsearch.painless.node;

import org.elasticsearch.painless.MethodWriter;
import org.elasticsearch.painless.Variables;

/**
 * Represents a function reference.
 */
public class EFunctionRef extends AExpression {
    public String type;
    public String call;

    public EFunctionRef(int line, int offset, String location, String type, String call) {
        super(line, offset, location);

        this.type = type;
        this.call = call;
    }

    @Override
    void analyze(Variables variables) {
        throw new UnsupportedOperationException(error("Function references [" + type + "::" + call + "] are not currently supported."));
    }

    @Override
    void write(MethodWriter writer) {
        throw new IllegalStateException(error("Illegal tree structure."));
    }
}

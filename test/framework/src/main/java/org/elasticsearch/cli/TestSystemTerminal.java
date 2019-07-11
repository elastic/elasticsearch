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

package org.elasticsearch.cli;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;

public class TestSystemTerminal extends Terminal {
    Terminal delegate = Terminal.DEFAULT;

    public TestSystemTerminal(InputStream inputStream) throws IOException {
        super(System.lineSeparator());
        this.delegate.setInput(inputStream);
        if (!"SystemTerminal".equals(this.delegate.getClass().getSimpleName())) {
            throw new AssertionError("delegate must be a SystemTerminal");
        }
    }

    @Override
    public String readText(String prompt) {
        return this.delegate.readText(prompt);
    }

    @Override
    public char[] readSecret(String prompt) {
        return this.delegate.readSecret(prompt);
    }

    @Override
    public PrintWriter getWriter() {
        return this.delegate.getWriter();
    }
}

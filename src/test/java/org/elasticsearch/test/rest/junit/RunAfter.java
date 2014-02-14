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
package org.elasticsearch.test.rest.junit;

import com.google.common.collect.Lists;
import org.junit.runners.model.MultipleFailureException;
import org.junit.runners.model.Statement;

import java.util.List;

/**
 * {@link Statement} that allows to run a specific statement after another one
 */
public class RunAfter extends Statement {

    private final Statement next;
    private final Statement after;

    public RunAfter(Statement next, Statement after) {
        this.next = next;
        this.after = after;
    }

    @Override
    public void evaluate() throws Throwable {
        List<Throwable> errors = Lists.newArrayList();
        try {
            next.evaluate();
        } catch (Throwable e) {
            errors.add(e);
        } finally {
            try {
                after.evaluate();
            } catch (Throwable e) {
                errors.add(e);
            }
        }
        MultipleFailureException.assertEmpty(errors);
    }
}

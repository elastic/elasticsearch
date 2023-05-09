/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

/*
 * Licensed to Ted Dunning under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.elasticsearch.tdigest;

import org.junit.runner.Result;
import org.junit.runner.notification.Failure;
import org.junit.runner.notification.RunListener;

import com.carrotsearch.randomizedtesting.RandomizedContext;

public final class ReproduceInfoPrinterRunListener extends RunListener {

    private boolean failed = false;

    @Override
    public void testFailure(Failure failure) {
        failed = true;
    }

    @Override
    public void testRunFinished(Result result) {
        if (failed) {
            printReproLine();
        }
        failed = false;
    }

    private void printReproLine() {
        final StringBuilder b = new StringBuilder();
        b.append("NOTE: reproduce with: mvn test -Dtests.seed=").append(RandomizedContext.current().getRunnerSeedAsString());
        if (System.getProperty("runSlowTests") != null) {
            b.append(" -DrunSlowTests=").append(System.getProperty("runSlowTests"));
        }
        b.append(" -Dtests.class=").append(RandomizedContext.current().getTargetClass().getName());
        System.out.println(b.toString());
    }

}

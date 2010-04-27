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

package org.elasticsearch.index.analysis;

import com.ibm.icu.text.Normalizer2;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.testng.annotations.Test;

import java.text.Normalizer;

/**
 * @author kimchy (shay.banon)
 */
public class Normalizer2Tests {

    @Test public void testNormalizer2() {
        Normalizer2 normalizer = Normalizer2.getInstance(null, "nfkc_cf", Normalizer2.Mode.COMPOSE);
        MatcherAssert.assertThat(normalizer.normalize("Jordania"), Matchers.equalTo("jordania"));
        MatcherAssert.assertThat(normalizer.normalize("João"), Matchers.equalTo("joão"));

        MatcherAssert.assertThat(Normalizer.normalize("Jordania", Normalizer.Form.NFKC), Matchers.equalTo("Jordania"));
        MatcherAssert.assertThat(Normalizer.normalize("João", Normalizer.Form.NFKC), Matchers.equalTo("João"));
    }
}

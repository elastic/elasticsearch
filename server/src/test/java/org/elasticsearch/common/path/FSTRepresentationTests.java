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

package org.elasticsearch.common.path;

import org.elasticsearch.rest.RestUtils;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

import java.util.HashMap;

public class PathTrieTests extends ESTestCase {

	public void testWithString() {
		FSTRepresentation<String> representation = new FSTRepresentation<String>();
		String in = "test";
		byte[] bytes = representation.toBytes(in);
		String out = representation.fromBytes(bytes);

		// The returned String should contain the same value
		assertThat(out, equalTo(in));
		// However, it is not the same object but instead a copy of it
		assertThat(out, is(not(sameInstance(in))));

		in = null;
		bytes = representation.toBytes(in);
		out = representation.fromBytes(bytes);

		// Should work for null-values too
		assertThat(out, equalTo(null));
	}

    public void testWithPrimitive() {
    	FSTRepresentation<Integer> representation = new FSTRepresentation<Integer>();
    	int in = 42;
    	byte[] bytes = representation.toBytes(in);
    	int out = representation.fromBytes(bytes);

    	// The returned integer should contain the same value
    	assertThat(out, equalTo(in));
    }

    public void testWithObject() {
    	FSTRepresentation<HashMap<String, Integer>> representation = new FSTRepresentation<HashMap<String, Integer>>();
    	HashMap<String, Integer> in = new HashMap<String, Integer>();
    	in.put("test1", 41);
    	in.put("test2", 42);
    	byte[] bytes = representation.toBytes(in);
    	HashMap<String, Integer> out = representation.fromBytes(bytes);

    	// The returned object should contain the same values
    	assertThat(out.get("test1"), equalTo(41));
    	assertThat(out.get("test2"), equalTo(42));
    }
}

package org.elasticsearch.test;

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

import com.carrotsearch.randomizedtesting.ThreadFilter;

import org.apache.lucene.util.Constants;
import org.apache.lucene.util.Version;

/** temporary workaround for https://issues.apache.org/jira/browse/LUCENE-6518
 *  remove me on upgrade to 5.3! I am just an updated version of QuickPatchThreadFilters from lucene */
public class IBMJ9HackThreadFilters implements ThreadFilter {
    static final boolean isJ9;
    
    static {
        assert Version.LATEST.equals(Version.LUCENE_5_2_1) : "please remove this entire class for 5.3";
        isJ9 = Constants.JAVA_VENDOR.startsWith("IBM");
    }
    
    @Override
    public boolean reject(Thread t) {
        if (isJ9) {
            // LUCENE-6518
            if ("ClassCache Reaper".equals(t.getName())) {
                return true;
            }
            
            // LUCENE-4736
            StackTraceElement [] stack = t.getStackTrace();
            if (stack.length > 0 && stack[stack.length - 1].getClassName().equals("java.util.Timer$TimerImpl")) {
                return true;
            }
        }
        return false;
    }
}

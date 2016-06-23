package org.elasticsearch.painless;

import java.util.function.Function;

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

/** Currently just a dummy class for testing a few features not yet exposed by whitelist! */
public class FeatureTest {
    private int x;
    private int y;
    
    /** empty ctor */
    public FeatureTest() {
    }
    
    /** ctor with params */
    public FeatureTest(int x, int y) {
        this.x = x;
        this.y = y;
    }

    /** getter for x */
    public int getX() {
        return x;
    }

    /** setter for x */
    public void setX(int x) {
        this.x = x;
    }

    /** getter for y */
    public int getY() {
        return y;
    }

    /** setter for y */
    public void setY(int y) {
        this.y = y;
    }

    /** static method that returns true */
    public static boolean overloadedStatic() {
        return true;
    }
    
    /** static method that returns what you ask it */
    public static boolean overloadedStatic(boolean whatToReturn) {
        return whatToReturn;
    }
    
    /** method taking two functions! */
    public Object twoFunctionsOfX(Function<Object,Object> f, Function<Object,Object> g) {
        return f.apply(g.apply(x));
    }
}

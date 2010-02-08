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

package org.elasticsearch.util.concurrent;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * GuardedBy
 * <p/>
 * The field or method to which this annotation is applied can only be accessed
 * when holding a particular lock, which may be a built-in (synchronization)
 * lock, or may be an explicit java.util.concurrent.Lock.
 * <p/>
 * The argument determines which lock guards the annotated field or method: this :
 * The string literal "this" means that this field is guarded by the class in
 * which it is defined. class-name.this : For inner classes, it may be necessary
 * to disambiguate 'this'; the class-name.this designation allows you to specify
 * which 'this' reference is intended itself : For reference fields only; the
 * object to which the field refers. field-name : The lock object is referenced
 * by the (instance or static) field specified by field-name.
 * class-name.field-name : The lock object is reference by the static field
 * specified by class-name.field-name. method-name() : The lock object is
 * returned by calling the named nil-ary method. class-name.class : The Class
 * object for the specified class should be used as the lock object.
 *
 * @author kimchy (Shay Banon)
 */
@Target({ElementType.FIELD, ElementType.METHOD})
@Retention(RetentionPolicy.CLASS)
public @interface GuardedBy {
    String value();
}
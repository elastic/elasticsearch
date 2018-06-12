/*
 * Copyright (C) 2008 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package org.elasticsearch.common.inject.multibindings;

import org.elasticsearch.common.inject.BindingAnnotation;

import java.lang.annotation.Retention;

import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * An internal binding annotation applied to each element in a multibinding.
 * All elements are assigned a globally-unique id to allow different modules
 * to contribute multibindings independently.
 *
 * @author jessewilson@google.com (Jesse Wilson)
 */
@Retention(RUNTIME)
@BindingAnnotation
@interface Element {
    String setName();

    int uniqueId();
}

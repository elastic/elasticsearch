/**
 * Copyright (C) 2007 Google Inc.
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

package org.elasticsearch.common.inject.assistedinject;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.CONSTRUCTOR;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * <p>Constructors annotated with {@code @AssistedInject} indicate that they can be instantiated by
 * the {@link FactoryProvider}. Each constructor must exactly match one corresponding factory method
 * within the factory interface.
 * <p/>
 * <p>Constructor parameters must be either supplied by the factory interface and marked with
 * <code>@Assisted</code>, or they must be injectable.
 *
 * @author jmourits@google.com (Jerome Mourits)
 * @author jessewilson@google.com (Jesse Wilson)
 * @deprecated {@link FactoryProvider} now works better with the standard {@literal @Inject}
 *             annotation. When using that annotation, parameters are matched by name and type rather than
 *             by position. In addition, values that use the standard {@literal @Inject} constructor
 *             annotation are eligible for method interception.
 */
@Target({CONSTRUCTOR})
@Retention(RUNTIME)
@Deprecated
public @interface AssistedInject {
}

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
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
package org.apache.lucene5_shaded.util;


import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * A utility for keeping backwards compatibility on previously abstract methods
 * (or similar replacements).
 * <p>Before the replacement method can be made abstract, the old method must kept deprecated.
 * If somebody still overrides the deprecated method in a non-final class,
 * you must keep track, of this and maybe delegate to the old method in the subclass.
 * The cost of reflection is minimized by the following usage of this class:</p>
 * <p>Define <strong>static final</strong> fields in the base class ({@code BaseClass}),
 * where the old and new method are declared:</p>
 * <pre class="prettyprint">
 *  static final VirtualMethod&lt;BaseClass&gt; newMethod =
 *   new VirtualMethod&lt;BaseClass&gt;(BaseClass.class, "newName", parameters...);
 *  static final VirtualMethod&lt;BaseClass&gt; oldMethod =
 *   new VirtualMethod&lt;BaseClass&gt;(BaseClass.class, "oldName", parameters...);
 * </pre>
 * <p>This enforces the singleton status of these objects, as the maintenance of the cache would be too costly else.
 * If you try to create a second instance of for the same method/{@code baseClass} combination, an exception is thrown.
 * <p>To detect if e.g. the old method was overridden by a more far subclass on the inheritance path to the current
 * instance's class, use a <strong>non-static</strong> field:</p>
 * <pre class="prettyprint">
 *  final boolean isDeprecatedMethodOverridden =
 *   oldMethod.getImplementationDistance(this.getClass()) &gt; newMethod.getImplementationDistance(this.getClass());
 *
 *  <em>// alternatively (more readable):</em>
 *  final boolean isDeprecatedMethodOverridden =
 *   VirtualMethod.compareImplementationDistance(this.getClass(), oldMethod, newMethod) &gt; 0
 * </pre> 
 * <p>{@link #getImplementationDistance} returns the distance of the subclass that overrides this method.
 * The one with the larger distance should be used preferable.
 * This way also more complicated method rename scenarios can be handled
 * (think of 2.9 {@code TokenStream} deprecations).</p>
 *
 * @lucene.internal
 */
public final class VirtualMethod<C> {

  private static final Set<Method> singletonSet = Collections.synchronizedSet(new HashSet<Method>());

  private final Class<C> baseClass;
  private final String method;
  private final Class<?>[] parameters;
  private final ClassValue<Integer> distanceOfClass = new ClassValue<Integer>() {
    @Override
    protected Integer computeValue(Class<?> subclazz) {
      return Integer.valueOf(reflectImplementationDistance(subclazz));
    }
  };

  /**
   * Creates a new instance for the given {@code baseClass} and method declaration.
   * @throws UnsupportedOperationException if you create a second instance of the same
   *  {@code baseClass} and method declaration combination. This enforces the singleton status.
   * @throws IllegalArgumentException if {@code baseClass} does not declare the given method.
   */
  public VirtualMethod(Class<C> baseClass, String method, Class<?>... parameters) {
    this.baseClass = baseClass;
    this.method = method;
    this.parameters = parameters;
    try {
      if (!singletonSet.add(baseClass.getDeclaredMethod(method, parameters)))
        throw new UnsupportedOperationException(
          "VirtualMethod instances must be singletons and therefore " +
          "assigned to static final members in the same class, they use as baseClass ctor param."
        );
    } catch (NoSuchMethodException nsme) {
      throw new IllegalArgumentException(baseClass.getName() + " has no such method: "+nsme.getMessage());
    }
  }
  
  /**
   * Returns the distance from the {@code baseClass} in which this method is overridden/implemented
   * in the inheritance path between {@code baseClass} and the given subclass {@code subclazz}.
   * @return 0 iff not overridden, else the distance to the base class
   */
  public int getImplementationDistance(final Class<? extends C> subclazz) {
    return distanceOfClass.get(subclazz).intValue();
  }
  
  /**
   * Returns, if this method is overridden/implemented in the inheritance path between
   * {@code baseClass} and the given subclass {@code subclazz}.
   * <p>You can use this method to detect if a method that should normally be final was overridden
   * by the given instance's class.
   * @return {@code false} iff not overridden
   */
  public boolean isOverriddenAsOf(final Class<? extends C> subclazz) {
    return getImplementationDistance(subclazz) > 0;
  }
  
  int reflectImplementationDistance(final Class<?> subclazz) {
    if (!baseClass.isAssignableFrom(subclazz))
      throw new IllegalArgumentException(subclazz.getName() + " is not a subclass of " + baseClass.getName());
    boolean overridden = false;
    int distance = 0;
    for (Class<?> clazz = subclazz; clazz != baseClass && clazz != null; clazz = clazz.getSuperclass()) {
      // lookup method, if success mark as overridden
      if (!overridden) {
        try {
          clazz.getDeclaredMethod(method, parameters);
          overridden = true;
        } catch (NoSuchMethodException nsme) {
        }
      }
      
      // increment distance if overridden
      if (overridden) distance++;
    }
    return distance;
  }
  
  /**
   * Utility method that compares the implementation/override distance of two methods.
   * @return <ul>
   *  <li>&gt; 1, iff {@code m1} is overridden/implemented in a subclass of the class overriding/declaring {@code m2}
   *  <li>&lt; 1, iff {@code m2} is overridden in a subclass of the class overriding/declaring {@code m1}
   *  <li>0, iff both methods are overridden in the same class (or are not overridden at all)
   * </ul>
   */
  public static <C> int compareImplementationDistance(final Class<? extends C> clazz,
    final VirtualMethod<C> m1, final VirtualMethod<C> m2)
  {
    return Integer.valueOf(m1.getImplementationDistance(clazz)).compareTo(m2.getImplementationDistance(clazz));
  }
  
}

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


import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;

import org.apache.lucene5_shaded.analysis.TokenStream; // for javadocs

/**
 * An AttributeSource contains a list of different {@link AttributeImpl}s,
 * and methods to add and get them. There can only be a single instance
 * of an attribute in the same AttributeSource instance. This is ensured
 * by passing in the actual type of the Attribute (Class&lt;Attribute&gt;) to 
 * the {@link #addAttribute(Class)}, which then checks if an instance of
 * that type is already present. If yes, it returns the instance, otherwise
 * it creates a new instance and returns it.
 */
public class AttributeSource {
  
  /**
   * This class holds the state of an AttributeSource.
   * @see #captureState
   * @see #restoreState
   */
  public static final class State implements Cloneable {
    AttributeImpl attribute;
    State next;
    
    @Override
    public State clone() {
      State clone = new State();
      clone.attribute = attribute.clone();
      
      if (next != null) {
        clone.next = next.clone();
      }
      
      return clone;
    }
  }
    
  // These two maps must always be in sync!!!
  // So they are private, final and read-only from the outside (read-only iterators)
  private final Map<Class<? extends Attribute>, AttributeImpl> attributes;
  private final Map<Class<? extends AttributeImpl>, AttributeImpl> attributeImpls;
  private final State[] currentState;

  private final AttributeFactory factory;
  
  /**
   * An AttributeSource using the default attribute factory {@link AttributeFactory#DEFAULT_ATTRIBUTE_FACTORY}.
   */
  public AttributeSource() {
    this(AttributeFactory.DEFAULT_ATTRIBUTE_FACTORY);
  }
  
  /**
   * An AttributeSource that uses the same attributes as the supplied one.
   */
  public AttributeSource(AttributeSource input) {
    Objects.requireNonNull(input, "input AttributeSource must not be null");
    this.attributes = input.attributes;
    this.attributeImpls = input.attributeImpls;
    this.currentState = input.currentState;
    this.factory = input.factory;
  }
  
  /**
   * An AttributeSource using the supplied {@link AttributeFactory} for creating new {@link Attribute} instances.
   */
  public AttributeSource(AttributeFactory factory) {
    this.attributes = new LinkedHashMap<>();
    this.attributeImpls = new LinkedHashMap<>();
    this.currentState = new State[1];
    this.factory = Objects.requireNonNull(factory, "AttributeFactory must not be null");
  }
  
  /**
   * returns the used AttributeFactory.
   */
  public final AttributeFactory getAttributeFactory() {
    return this.factory;
  }
  
  /** Returns a new iterator that iterates the attribute classes
   * in the same order they were added in.
   */
  public final Iterator<Class<? extends Attribute>> getAttributeClassesIterator() {
    return Collections.unmodifiableSet(attributes.keySet()).iterator();
  }
  
  /** Returns a new iterator that iterates all unique Attribute implementations.
   * This iterator may contain less entries that {@link #getAttributeClassesIterator},
   * if one instance implements more than one Attribute interface.
   */
  public final Iterator<AttributeImpl> getAttributeImplsIterator() {
    final State initState = getCurrentState();
    if (initState != null) {
      return new Iterator<AttributeImpl>() {
        private State state = initState;
      
        @Override
        public void remove() {
          throw new UnsupportedOperationException();
        }
        
        @Override
        public AttributeImpl next() {
          if (state == null)
            throw new NoSuchElementException();
          final AttributeImpl att = state.attribute;
          state = state.next;
          return att;
        }
        
        @Override
        public boolean hasNext() {
          return state != null;
        }
      };
    } else {
      return Collections.<AttributeImpl>emptySet().iterator();
    }
  }
  
  /** a cache that stores all interfaces for known implementation classes for performance (slow reflection) */
  private static final ClassValue<Class<? extends Attribute>[]> implInterfaces = new ClassValue<Class<? extends Attribute>[]>() {
    @Override
    protected Class<? extends Attribute>[] computeValue(Class<?> clazz) {
      final Set<Class<? extends Attribute>> intfSet = new LinkedHashSet<>();
      // find all interfaces that this attribute instance implements
      // and that extend the Attribute interface
      do {
        for (Class<?> curInterface : clazz.getInterfaces()) {
          if (curInterface != Attribute.class && Attribute.class.isAssignableFrom(curInterface)) {
            intfSet.add(curInterface.asSubclass(Attribute.class));
          }
        }
        clazz = clazz.getSuperclass();
      } while (clazz != null);
      @SuppressWarnings({"unchecked", "rawtypes"}) final Class<? extends Attribute>[] a =
          intfSet.toArray(new Class[intfSet.size()]);
      return a;
    }
  };
  
  static Class<? extends Attribute>[] getAttributeInterfaces(final Class<? extends AttributeImpl> clazz) {
    return implInterfaces.get(clazz);
  }
  
  /** <b>Expert:</b> Adds a custom AttributeImpl instance with one or more Attribute interfaces.
   * <p><b>NOTE:</b> It is not guaranteed, that <code>att</code> is added to
   * the <code>AttributeSource</code>, because the provided attributes may already exist.
   * You should always retrieve the wanted attributes using {@link #getAttribute} after adding
   * with this method and cast to your class.
   * The recommended way to use custom implementations is using an {@link AttributeFactory}.
   * </p>
   */
  public final void addAttributeImpl(final AttributeImpl att) {
    final Class<? extends AttributeImpl> clazz = att.getClass();
    if (attributeImpls.containsKey(clazz)) return;
    
    // add all interfaces of this AttributeImpl to the maps
    for (final Class<? extends Attribute> curInterface : getAttributeInterfaces(clazz)) {
      // Attribute is a superclass of this interface
      if (!attributes.containsKey(curInterface)) {
        // invalidate state to force recomputation in captureState()
        this.currentState[0] = null;
        attributes.put(curInterface, att);
        attributeImpls.put(clazz, att);
      }
    }
  }
  
  /**
   * The caller must pass in a Class&lt;? extends Attribute&gt; value.
   * This method first checks if an instance of that class is 
   * already in this AttributeSource and returns it. Otherwise a
   * new instance is created, added to this AttributeSource and returned. 
   */
  public final <T extends Attribute> T addAttribute(Class<T> attClass) {
    AttributeImpl attImpl = attributes.get(attClass);
    if (attImpl == null) {
      if (!(attClass.isInterface() && Attribute.class.isAssignableFrom(attClass))) {
        throw new IllegalArgumentException(
          "addAttribute() only accepts an interface that extends Attribute, but " +
          attClass.getName() + " does not fulfil this contract."
        );
      }
      addAttributeImpl(attImpl = this.factory.createAttributeInstance(attClass));
    }
    return attClass.cast(attImpl);
  }
  
  /** Returns true, iff this AttributeSource has any attributes */
  public final boolean hasAttributes() {
    return !this.attributes.isEmpty();
  }

  /**
   * The caller must pass in a Class&lt;? extends Attribute&gt; value. 
   * Returns true, iff this AttributeSource contains the passed-in Attribute.
   */
  public final boolean hasAttribute(Class<? extends Attribute> attClass) {
    return this.attributes.containsKey(attClass);
  }

  /**
   * Returns the instance of the passed in Attribute contained in this AttributeSource
   * <p>
   * The caller must pass in a Class&lt;? extends Attribute&gt; value. 
   * 
   * @return instance of the passed in Attribute, or {@code null} if this AttributeSource 
   *         does not contain the Attribute. It is recommended to always use 
   *         {@link #addAttribute} even in consumers  of TokenStreams, because you cannot 
   *         know if a specific TokenStream really uses a specific Attribute. 
   *         {@link #addAttribute} will automatically make the attribute available. 
   *         If you want to only use the attribute, if it is available (to optimize
   *         consuming), use {@link #hasAttribute}.
   */
  public final <T extends Attribute> T getAttribute(Class<T> attClass) {
    return attClass.cast(attributes.get(attClass));
  }
    
  private State getCurrentState() {
    State s  = currentState[0];
    if (s != null || !hasAttributes()) {
      return s;
    }
    State c = s = currentState[0] = new State();
    final Iterator<AttributeImpl> it = attributeImpls.values().iterator();
    c.attribute = it.next();
    while (it.hasNext()) {
      c.next = new State();
      c = c.next;
      c.attribute = it.next();
    }
    return s;
  }
  
  /**
   * Resets all Attributes in this AttributeSource by calling
   * {@link AttributeImpl#clear()} on each Attribute implementation.
   */
  public final void clearAttributes() {
    for (State state = getCurrentState(); state != null; state = state.next) {
      state.attribute.clear();
    }
  }
  
  /**
   * Resets all Attributes in this AttributeSource by calling
   * {@link AttributeImpl#end()} on each Attribute implementation.
   */
  public final void endAttributes() {
    for (State state = getCurrentState(); state != null; state = state.next) {
      state.attribute.end();
    }
  }

  /**
   * Removes all attributes and their implementations from this AttributeSource.
   */
  public final void removeAllAttributes() {
    attributes.clear();
    attributeImpls.clear();
  }

  /**
   * Captures the state of all Attributes. The return value can be passed to
   * {@link #restoreState} to restore the state of this or another AttributeSource.
   */
  public final State captureState() {
    final State state = this.getCurrentState();
    return (state == null) ? null : state.clone();
  }
  
  /**
   * Restores this state by copying the values of all attribute implementations
   * that this state contains into the attributes implementations of the targetStream.
   * The targetStream must contain a corresponding instance for each argument
   * contained in this state (e.g. it is not possible to restore the state of
   * an AttributeSource containing a TermAttribute into a AttributeSource using
   * a Token instance as implementation).
   * <p>
   * Note that this method does not affect attributes of the targetStream
   * that are not contained in this state. In other words, if for example
   * the targetStream contains an OffsetAttribute, but this state doesn't, then
   * the value of the OffsetAttribute remains unchanged. It might be desirable to
   * reset its value to the default, in which case the caller should first
   * call {@link TokenStream#clearAttributes()} on the targetStream.   
   */
  public final void restoreState(State state) {
    if (state == null)  return;
    
    do {
      AttributeImpl targetImpl = attributeImpls.get(state.attribute.getClass());
      if (targetImpl == null) {
        throw new IllegalArgumentException("State contains AttributeImpl of type " +
          state.attribute.getClass().getName() + " that is not in in this AttributeSource");
      }
      state.attribute.copyTo(targetImpl);
      state = state.next;
    } while (state != null);
  }

  @Override
  public int hashCode() {
    int code = 0;
    for (State state = getCurrentState(); state != null; state = state.next) {
      code = code * 31 + state.attribute.hashCode();
    }
    return code;
  }
  
  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    }

    if (obj instanceof AttributeSource) {
      AttributeSource other = (AttributeSource) obj;  
    
      if (hasAttributes()) {
        if (!other.hasAttributes()) {
          return false;
        }
        
        if (this.attributeImpls.size() != other.attributeImpls.size()) {
          return false;
        }
  
        // it is only equal if all attribute impls are the same in the same order
        State thisState = this.getCurrentState();
        State otherState = other.getCurrentState();
        while (thisState != null && otherState != null) {
          if (otherState.attribute.getClass() != thisState.attribute.getClass() || !otherState.attribute.equals(thisState.attribute)) {
            return false;
          }
          thisState = thisState.next;
          otherState = otherState.next;
        }
        return true;
      } else {
        return !other.hasAttributes();
      }
    } else
      return false;
  }
  
  /**
   * This method returns the current attribute values as a string in the following format
   * by calling the {@link #reflectWith(AttributeReflector)} method:
   * 
   * <ul>
   * <li><em>iff {@code prependAttClass=true}:</em> {@code "AttributeClass#key=value,AttributeClass#key=value"}
   * <li><em>iff {@code prependAttClass=false}:</em> {@code "key=value,key=value"}
   * </ul>
   *
   * @see #reflectWith(AttributeReflector)
   */
  public final String reflectAsString(final boolean prependAttClass) {
    final StringBuilder buffer = new StringBuilder();
    reflectWith(new AttributeReflector() {
      @Override
      public void reflect(Class<? extends Attribute> attClass, String key, Object value) {
        if (buffer.length() > 0) {
          buffer.append(',');
        }
        if (prependAttClass) {
          buffer.append(attClass.getName()).append('#');
        }
        buffer.append(key).append('=').append((value == null) ? "null" : value);
      }
    });
    return buffer.toString();
  }
  
  /**
   * This method is for introspection of attributes, it should simply
   * add the key/values this AttributeSource holds to the given {@link AttributeReflector}.
   *
   * <p>This method iterates over all Attribute implementations and calls the
   * corresponding {@link AttributeImpl#reflectWith} method.</p>
   *
   * @see AttributeImpl#reflectWith
   */
  public final void reflectWith(AttributeReflector reflector) {
    for (State state = getCurrentState(); state != null; state = state.next) {
      state.attribute.reflectWith(reflector);
    }
  }

  /**
   * Performs a clone of all {@link AttributeImpl} instances returned in a new
   * {@code AttributeSource} instance. This method can be used to e.g. create another TokenStream
   * with exactly the same attributes (using {@link #AttributeSource(AttributeSource)}).
   * You can also use it as a (non-performant) replacement for {@link #captureState}, if you need to look
   * into / modify the captured state.
   */
  public final AttributeSource cloneAttributes() {
    final AttributeSource clone = new AttributeSource(this.factory);
    
    if (hasAttributes()) {
      // first clone the impls
      for (State state = getCurrentState(); state != null; state = state.next) {
        clone.attributeImpls.put(state.attribute.getClass(), state.attribute.clone());
      }
      
      // now the interfaces
      for (Entry<Class<? extends Attribute>, AttributeImpl> entry : this.attributes.entrySet()) {
        clone.attributes.put(entry.getKey(), clone.attributeImpls.get(entry.getValue().getClass()));
      }
    }
    
    return clone;
  }
  
  /**
   * Copies the contents of this {@code AttributeSource} to the given target {@code AttributeSource}.
   * The given instance has to provide all {@link Attribute}s this instance contains. 
   * The actual attribute implementations must be identical in both {@code AttributeSource} instances;
   * ideally both AttributeSource instances should use the same {@link AttributeFactory}.
   * You can use this method as a replacement for {@link #restoreState}, if you use
   * {@link #cloneAttributes} instead of {@link #captureState}.
   */
  public final void copyTo(AttributeSource target) {
    for (State state = getCurrentState(); state != null; state = state.next) {
      final AttributeImpl targetImpl = target.attributeImpls.get(state.attribute.getClass());
      if (targetImpl == null) {
        throw new IllegalArgumentException("This AttributeSource contains AttributeImpl of type " +
          state.attribute.getClass().getName() + " that is not in the target");
      }
      state.attribute.copyTo(targetImpl);
    }
  }

  /**
   * Returns a string consisting of the class's simple name, the hex representation of the identity hash code,
   * and the current reflection of all attributes.
   * @see #reflectAsString(boolean)
   */
  @Override
  public String toString() {
    return getClass().getSimpleName() + '@' + Integer.toHexString(System.identityHashCode(this)) + " " + reflectAsString(false);
  }
}

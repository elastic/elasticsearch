/**
 * Copyright (c) 2004-2011 QOS.ch
 * All rights reserved.
 *
 * Permission is hereby granted, free  of charge, to any person obtaining
 * a  copy  of this  software  and  associated  documentation files  (the
 * "Software"), to  deal in  the Software without  restriction, including
 * without limitation  the rights to  use, copy, modify,  merge, publish,
 * distribute,  sublicense, and/or sell  copies of  the Software,  and to
 * permit persons to whom the Software  is furnished to do so, subject to
 * the following conditions:
 *
 * The  above  copyright  notice  and  this permission  notice  shall  be
 * included in all copies or substantial portions of the Software.
 *
 * THE  SOFTWARE IS  PROVIDED  "AS  IS", WITHOUT  WARRANTY  OF ANY  KIND,
 * EXPRESS OR  IMPLIED, INCLUDING  BUT NOT LIMITED  TO THE  WARRANTIES OF
 * MERCHANTABILITY,    FITNESS    FOR    A   PARTICULAR    PURPOSE    AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
 * LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 * OF CONTRACT, TORT OR OTHERWISE,  ARISING FROM, OUT OF OR IN CONNECTION
 * WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 *
 */
package org.slf4j.spi;

import java.util.Map;

/**
 * This interface abstracts the service offered by various MDC
 * implementations.
 * 
 * @author Ceki G&uuml;lc&uuml;
 * @since 1.4.1
 */
public interface MDCAdapter {

    /**
     * Put a context value (the <code>val</code> parameter) as identified with
     * the <code>key</code> parameter into the current thread's context map. 
     * The <code>key</code> parameter cannot be null. The <code>val</code> parameter
     * can be null only if the underlying implementation supports it.
     * 
     * <p>If the current thread does not have a context map it is created as a side
     * effect of this call.
     */
    public void put(String key, String val);

    /**
     * Get the context identified by the <code>key</code> parameter.
     * The <code>key</code> parameter cannot be null.
     * 
     * @return the string value identified by the <code>key</code> parameter.
     */
    public String get(String key);

    /**
     * Remove the the context identified by the <code>key</code> parameter. 
     * The <code>key</code> parameter cannot be null. 
     * 
     * <p>
     * This method does nothing if there is no previous value 
     * associated with <code>key</code>.
     */
    public void remove(String key);

    /**
     * Clear all entries in the MDC.
     */
    public void clear();

    /**
     * Return a copy of the current thread's context map, with keys and 
     * values of type String. Returned value may be null.
     * 
     * @return A copy of the current thread's context map. May be null.
     * @since 1.5.1
     */
    public Map<String, String> getCopyOfContextMap();

    /**
     * Set the current thread's context map by first clearing any existing 
     * map and then copying the map passed as parameter. The context map 
     * parameter must only contain keys and values of type String.
     * 
     * @param contextMap must contain only keys and values of type String
     * 
     * @since 1.5.1
     */
    public void setContextMap(Map<String, String> contextMap);
}

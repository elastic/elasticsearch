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

import org.slf4j.ILoggerFactory;

/**
 * An internal interface which helps the static {@link org.slf4j.LoggerFactory} 
 * class bind with the appropriate {@link ILoggerFactory} instance. 
 * 
 * @author Ceki G&uuml;lc&uuml;
 */
public interface LoggerFactoryBinder {

    /**
     * Return the instance of {@link ILoggerFactory} that 
     * {@link org.slf4j.LoggerFactory} class should bind to.
     * 
     * @return the instance of {@link ILoggerFactory} that 
     * {@link org.slf4j.LoggerFactory} class should bind to.
     */
    public ILoggerFactory getLoggerFactory();

    /**
     * The String form of the {@link ILoggerFactory} object that this 
     * <code>LoggerFactoryBinder</code> instance is <em>intended</em> to return. 
     * 
     * <p>This method allows the developer to interrogate this binder's intention
     * which may be different from the {@link ILoggerFactory} instance it is able to 
     * yield in practice. The discrepancy should only occur in case of errors.
     * 
     * @return the class name of the intended {@link ILoggerFactory} instance
     */
    public String getLoggerFactoryClassStr();
}

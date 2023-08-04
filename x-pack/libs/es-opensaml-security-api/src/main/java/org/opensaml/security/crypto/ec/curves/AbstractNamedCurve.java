/*
 * Licensed to the University Corporation for Advanced Internet Development,
 * Inc. (UCAID) under one or more contributor license agreements.  See the
 * NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The UCAID licenses this file to You under the Apache
 * License, Version 2.0 (the "License"); you may not use this file except in
 * compliance with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opensaml.security.crypto.ec.curves;

import net.shibboleth.utilities.java.support.annotation.constraint.NonnullAfterInit;
import net.shibboleth.utilities.java.support.component.AbstractInitializableComponent;
import net.shibboleth.utilities.java.support.component.ComponentInitializationException;
import net.shibboleth.utilities.java.support.component.ComponentSupport;

import org.opensaml.security.crypto.JCAConstants;
import org.opensaml.security.crypto.KeySupport;
import org.opensaml.security.crypto.ec.NamedCurve;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.interfaces.ECPublicKey;
import java.security.spec.ECGenParameterSpec;
import java.security.spec.ECParameterSpec;

import javax.annotation.Nullable;

/**
 * Abstract base class for implementations of {@link NamedCurve}.
 */
public abstract class AbstractNamedCurve extends AbstractInitializableComponent implements NamedCurve {

    /** Logger. */
    private final Logger log = LoggerFactory.getLogger(this.getClass());

    /** Instance of {@link ECParameterSpec} corresponding to the curve. */
    @NonnullAfterInit
    private ECParameterSpec paramSpec;

    /** {@inheritDoc} */
    @NonnullAfterInit
    public ECParameterSpec getParameterSpec() {
        ComponentSupport.ifNotInitializedThrowUninitializedComponentException(this);
        return paramSpec;
    }

    /** {@inheritDoc} */
    protected void doInitialize() throws ComponentInitializationException {
        super.doInitialize();

        paramSpec = buildParameterSpec();

        // This should never happen for any correctly-specified named curve that we'd actually define...
        if (paramSpec == null) {
            throw new ComponentInitializationException("Could not init NamedCurve ECParameterSpec");
        }
    }

    /**
     * Build an instance of {@link ECParameterSpec} corresponding to this curve.
     *
     * MODIFIED BY ELASTIC - We do not do the first step that consults Bouncy Castle
     * <p>
     * The default implementation here is that it first attempts to resolve the curve from
     * Bouncy Castle's {@code ECNamedCurveTable}.  If that is unsuccessful then it attempts
     * a brute force approach by generating a key pair using a {@link ECGenParameterSpec} based
     * on the curve's name from {@link #getName()}, returning the parameter instance from the
     * resulting {@link ECPublicKey}.
     * </p>
     *
     * @return the parameter spec instance, or null if can not be built
     */
    @Nullable
    protected ECParameterSpec buildParameterSpec() {
        try {
            var jcaSpec = ECPublicKey.class.cast(
                KeySupport.generateKeyPair(JCAConstants.KEY_ALGO_EC, new ECGenParameterSpec(getName()), null).getPublic()
            ).getParams();
            log.trace(
                "Inited NamedCurve ECParameterSpec via key pair generation for name '{}', OID '{}'",
                getName(),
                getObjectIdentifier()
            );
            return jcaSpec;
        } catch (final Exception e) {
            log.warn("Error initing the NamedCurve ECParameterSpce via key pair generation with name: {}", getName(), e);
        }

        log.warn(
            "Failed to init NamedCurve ECParameterSpec from BC or key pair generation for name '{}', OID '{}'",
            getName(),
            getObjectIdentifier()
        );

        return null;
    }

    /** {@inheritDoc} */
    public String toString() {
        return getClass().getSimpleName() + "{name=" + getName() + ", OID=" + getObjectIdentifier() + "}";
    }

}

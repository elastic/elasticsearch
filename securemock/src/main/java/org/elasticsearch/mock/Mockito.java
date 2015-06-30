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

package org.elasticsearch.mock;

import org.mockito.InOrder;
import org.mockito.Matchers;
import org.mockito.MockSettings;
import org.mockito.MockingDetails;
import org.mockito.ReturnValues;
import org.mockito.stubbing.Answer;
import org.mockito.stubbing.DeprecatedOngoingStubbing;
import org.mockito.stubbing.OngoingStubbing;
import org.mockito.stubbing.Stubber;
import org.mockito.stubbing.VoidMethodStubbable;
import org.mockito.verification.VerificationMode;
import org.mockito.verification.VerificationWithTimeout;

import java.security.AccessController;
import java.security.PrivilegedAction;

/**
 * Wraps Mockito API with calls to AccessController.
 * <p>
 * This is useful if you want to mock in a securitymanager environment,
 * but contain the permissions to only mocking test libraries.
 * <p>
 * Instead of:
 * <pre>
 * grant {
 *   permission java.lang.RuntimePermission "reflectionFactoryAccess";
 * };
 * </pre>
 * You can just change maven dependencies to use securemock.jar, and then:
 * <pre>
 * grant codeBase "/url/to/securemock.jar" {
 *   permission java.lang.RuntimePermission "reflectionFactoryAccess";
 * };
 * </pre>
 */
public class Mockito extends Matchers {
    
    public static final Answer<Object> RETURNS_DEFAULTS = org.mockito.Mockito.RETURNS_DEFAULTS;
    public static final Answer<Object> RETURNS_SMART_NULLS = org.mockito.Mockito.RETURNS_SMART_NULLS;
    public static final Answer<Object> RETURNS_MOCKS = org.mockito.Mockito.RETURNS_MOCKS;
    public static final Answer<Object> RETURNS_DEEP_STUBS = org.mockito.Mockito.RETURNS_DEEP_STUBS;
    public static final Answer<Object> CALLS_REAL_METHODS = org.mockito.Mockito.CALLS_REAL_METHODS;
    
    public static <T> T mock(final Class<T> classToMock) {
        return AccessController.doPrivileged(new PrivilegedAction<T>() {
            @Override
            public T run() {
                return org.mockito.Mockito.mock(classToMock);
            }
        });
    }
    
    public static <T> T mock(final Class<T> classToMock, final String name) {
        return AccessController.doPrivileged(new PrivilegedAction<T>() {
            @Override
            public T run() {
                return org.mockito.Mockito.mock(classToMock, name);
            }
        });
    }
    
    public static MockingDetails mockingDetails(final Object toInspect) {
        return AccessController.doPrivileged(new PrivilegedAction<MockingDetails>() {
            @Override
            public MockingDetails run() {
                return org.mockito.Mockito.mockingDetails(toInspect);
            }
        });
    }
    
    @Deprecated
    public static <T> T mock(final Class<T> classToMock, final ReturnValues returnValues) {
        return AccessController.doPrivileged(new PrivilegedAction<T>() {
            @Override
            public T run() {
                return org.mockito.Mockito.mock(classToMock, returnValues);
            }
        });
    }
    
    public static <T> T mock(final Class<T> classToMock, final Answer defaultAnswer) {
        return AccessController.doPrivileged(new PrivilegedAction<T>() {
            @Override
            public T run() {
                return org.mockito.Mockito.mock(classToMock, defaultAnswer);
            }
        });
    }
    
    public static <T> T mock(final Class<T> classToMock, final MockSettings mockSettings) {
        return AccessController.doPrivileged(new PrivilegedAction<T>() {
            @Override
            public T run() {
                return org.mockito.Mockito.mock(classToMock, mockSettings);
            }
        });
    }
    
    public static <T> T spy(final T object) {
        return AccessController.doPrivileged(new PrivilegedAction<T>() {
            @Override
            public T run() {
                return org.mockito.Mockito.spy(object);
            }
        });
    }
    
    public static <T> DeprecatedOngoingStubbing<T> stub(final T methodCall) {
        return AccessController.doPrivileged(new PrivilegedAction<DeprecatedOngoingStubbing<T>>() {
            @Override
            public DeprecatedOngoingStubbing<T> run() {
                return org.mockito.Mockito.stub(methodCall);
            }
        });
    }
    
    public static <T> OngoingStubbing<T> when(final T methodCall) {
        return AccessController.doPrivileged(new PrivilegedAction<OngoingStubbing<T>>() {
            @Override
            public OngoingStubbing<T> run() {
                return org.mockito.Mockito.when(methodCall);
            }
        });
    }
    
    public static <T> T verify(final T mock) {
        return AccessController.doPrivileged(new PrivilegedAction<T>() {
            @Override
            public T run() {
                return org.mockito.Mockito.verify(mock);
            }
        });
    }
    
    public static <T> T verify(final T mock, final VerificationMode mode) {
        return AccessController.doPrivileged(new PrivilegedAction<T>() {
            @Override
            public T run() {
                return org.mockito.Mockito.verify(mock, mode);
            }
        });
    }
    
    public static <T> void reset(final T ... mocks) {
        AccessController.doPrivileged(new PrivilegedAction<Void>() {
            @Override
            public Void run() {
                org.mockito.Mockito.reset(mocks);
                return null;
            }
        });
    }
    
    public static void verifyNoMoreInteractions(final Object... mocks) {
        AccessController.doPrivileged(new PrivilegedAction<Void>() {
            @Override
            public Void run() {
                org.mockito.Mockito.verifyNoMoreInteractions(mocks);
                return null;
            }
        });
    }
    
    public static void verifyZeroInteractions(final Object... mocks) {
        AccessController.doPrivileged(new PrivilegedAction<Void>() {
            @Override
            public Void run() {
                org.mockito.Mockito.verifyZeroInteractions(mocks);
                return null;
            }
        });
    }
    
    @Deprecated
    public static <T> VoidMethodStubbable<T> stubVoid(final T mock) {
        return AccessController.doPrivileged(new PrivilegedAction<VoidMethodStubbable<T>>() {
            @Override
            public VoidMethodStubbable<T> run() {
                return org.mockito.Mockito.stubVoid(mock);
            }
        });
    }
    
    public static Stubber doThrow(final Throwable toBeThrown) {
        return AccessController.doPrivileged(new PrivilegedAction<Stubber>() {
            @Override
            public Stubber run() {
                return org.mockito.Mockito.doThrow(toBeThrown);
            }
        });
    }
    
    public static Stubber doThrow(final Class<? extends Throwable> toBeThrown) {
        return AccessController.doPrivileged(new PrivilegedAction<Stubber>() {
            @Override
            public Stubber run() {
                return org.mockito.Mockito.doThrow(toBeThrown);
            }
        });
    }
    
    public static Stubber doCallRealMethod() {
        return AccessController.doPrivileged(new PrivilegedAction<Stubber>() {
            @Override
            public Stubber run() {
                return org.mockito.Mockito.doCallRealMethod();
            }
        });
    }
    
    public static Stubber doAnswer(final Answer answer) {
        return AccessController.doPrivileged(new PrivilegedAction<Stubber>() {
            @Override
            public Stubber run() {
                return org.mockito.Mockito.doAnswer(answer);
            }
        });
    }  
    
    public static Stubber doNothing() {
        return AccessController.doPrivileged(new PrivilegedAction<Stubber>() {
            @Override
            public Stubber run() {
                return org.mockito.Mockito.doNothing();
            }
        });
    }
    
    public static Stubber doReturn(final Object toBeReturned) {
        return AccessController.doPrivileged(new PrivilegedAction<Stubber>() {
            @Override
            public Stubber run() {
                return org.mockito.Mockito.doReturn(toBeReturned);
            }
        });
    }
    
    public static InOrder inOrder(final Object... mocks) {
        return AccessController.doPrivileged(new PrivilegedAction<InOrder>() {
            @Override
            public InOrder run() {
                return org.mockito.Mockito.inOrder(mocks);
            }
        });
    }
    
    public static Object[] ignoreStubs(final Object... mocks) {
        return AccessController.doPrivileged(new PrivilegedAction<Object[]>() {
            @Override
            public Object[] run() {
                return org.mockito.Mockito.ignoreStubs(mocks);
            }
        });
    }
    
    public static VerificationMode times(final int wantedNumberOfInvocations) {
        return AccessController.doPrivileged(new PrivilegedAction<VerificationMode>() {
            @Override
            public VerificationMode run() {
                return org.mockito.Mockito.times(wantedNumberOfInvocations);
            }
        });
    }
    
    public static VerificationMode never() {
        return AccessController.doPrivileged(new PrivilegedAction<VerificationMode>() {
            @Override
            public VerificationMode run() {
                return org.mockito.Mockito.never();
            }
        });
    }
    
    public static VerificationMode atLeastOnce() {
        return AccessController.doPrivileged(new PrivilegedAction<VerificationMode>() {
            @Override
            public VerificationMode run() {
                return org.mockito.Mockito.atLeastOnce();
            }
        });
    }
    
    public static VerificationMode atLeast(final int minNumberOfInvocations) {
        return AccessController.doPrivileged(new PrivilegedAction<VerificationMode>() {
            @Override
            public VerificationMode run() {
                return org.mockito.Mockito.atLeast(minNumberOfInvocations);
            }
        });
    }
    
    public static VerificationMode atMost(final int maxNumberOfInvocations) {
        return AccessController.doPrivileged(new PrivilegedAction<VerificationMode>() {
            @Override
            public VerificationMode run() {
                return org.mockito.Mockito.atMost(maxNumberOfInvocations);
            }
        });
    }
    
    public static VerificationMode calls(final int wantedNumberOfInvocations) {
        return AccessController.doPrivileged(new PrivilegedAction<VerificationMode>() {
            @Override
            public VerificationMode run() {
                return org.mockito.Mockito.calls(wantedNumberOfInvocations);
            }
        });
    }
    
    public static VerificationMode only() {
        return AccessController.doPrivileged(new PrivilegedAction<VerificationMode>() {
            @Override
            public VerificationMode run() {
                return org.mockito.Mockito.only();
            }
        });
    }
    
    public static VerificationWithTimeout timeout(final int millis) {
        return AccessController.doPrivileged(new PrivilegedAction<VerificationWithTimeout>() {
            @Override
            public VerificationWithTimeout run() {
                return org.mockito.Mockito.timeout(millis);
            }
        });
    }
    
    public static void validateMockitoUsage() {
        AccessController.doPrivileged(new PrivilegedAction<Void>() {
            @Override
            public Void run() {
                org.mockito.Mockito.validateMockitoUsage();
                return null;
            }
        });
    }
    
    public static MockSettings withSettings() {
        return AccessController.doPrivileged(new PrivilegedAction<MockSettings>() {
            @Override
            public MockSettings run() {
                return org.mockito.Mockito.withSettings();
            }
        });
    }
}

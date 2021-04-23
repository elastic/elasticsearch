Slow tests are placed in this folder so that they don't get picked up accidently through the classpath discovery.
A good example are frozen tests, which by their nature, take a LOT more time to execute and thus would cause
the other 'normal' tests to time-out.
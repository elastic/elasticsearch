# Explanation of files in this directory

`rsa-private-jwkset.json`, `rsa-public-jwkset.json`, `hmac-jwkset.json`
-----------------------------------------------------------------------

These files are created by running the tests in `JwtRealmGenerateTests`.

Those tests generate the yaml settings, the keystore settings and the JWK Sets
for each sample reaml

Copy the output from the test output into the applicable file (you may wish to
run it through `jq` first in order to make it more readable).

-------

If additional keys are needed (e.g. to add more algorithms / key sizes) we can
either extend the existing JWKSets with another set of keys (that is, modify the
existing method in `JwtRealmGenerateTests` so that it creates more keys in the
same JWKSet, then re-run and replace the files on disk) or create new files (
that is, add another test to `JwtRealmGenerateTests` that creates the relevant
realm and/or JWKSet, then re-run and add the new files to disk).

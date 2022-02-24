# Explanation of files in this directory

`rsa-private-jwkset.json` and `rsa-public-jwkset.json`
------------------------------------------------------

These files are created by running the `JwkSetGenerator` that is contained in the `java/` source tree of this project.

Copy the output from that Java utility into the applicable file (you may wish to run it through `jq` first in order to make it more readable).

-------

If additional keys are needed (e.g. to add more algorithms / key sizes) we can either extend the existing JWKSets with another set of keys (that is, modify `JwkSetGenerator` so that it creates more keys in the same JWKSet, then re-run and replace the files on disk) or create new files (that is, modify `JwkSetGenerator` to build & print another JWKSet, then re-run and add the new files to disk).
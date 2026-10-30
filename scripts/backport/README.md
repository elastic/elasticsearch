# Backport wrapper

Runs `backport` and automatically handles muted-tests.yml conflicts.

## Dependencies

- Node 24+

## Setup

```bash
cd scripts/backport
npm install
```

## Usage

Replace `backport <args>` with `./script/backport.sh <args>`

e.g.

```bash
./scripts/backport.sh --pr 12345
```

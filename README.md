# YellowDog Go SDK: alpha release

This repository holds alpha builds of the YellowDog Go SDK. The API may change before the first tagged release.
[OVERVIEW.md](OVERVIEW.md) summarises what the SDK covers.

## Using the SDK

The SDK requires Go 1.25 or later.

1. Add the SDK to your module:

   ```bash
   go mod edit -require=github.com/yellowdog/yellowdog-sdk-go@v0.0.0 \
     -replace=github.com/yellowdog/yellowdog-sdk-go=github.com/yellowdog/yellowdog-sdk-go-alpha@main
   go mod tidy
   ```

   The SDK's import path is `github.com/yellowdog/yellowdog-sdk-go`, so the `replace` directive tells Go to fetch
   it from this repository. `go mod tidy` pins it to the latest commit. Run both commands again to upgrade.

2. Create a client. The SDK reads `YELLOWDOG_API_KEY` and `YELLOWDOG_API_SECRET` from the environment, and
   optionally `YELLOWDOG_API_URL`:

   ```go
   client, err := yellowdog.NewClient(yellowdog.DefaultConfig())
   ```

   Each service is a field on the client, such as `client.Compute`, `client.Work` or `client.WorkerPool`.

3. Read the documentation. The package docs and runnable examples are in the source. Browse them from your module
   with `go doc github.com/yellowdog/yellowdog-sdk-go`. The links to pkg.go.dev in `OVERVIEW.md` won't work until
   the SDK is published.

## Known issues

All of these issues will be fixed before the first stable release.

### 1. Polymorphic types are not type-checked

Polymorphic model types, such as `model.ComputeSource`, are plain Go interfaces that unrelated SDK types can also
satisfy. Passing the wrong type compiles, and fails only when the platform rejects the request.

### 2. Method names may change between releases

Any method may be renamed in a later release. The most likely to change are those for operations with several
forms, which are told apart by suffixes such as `ById`, `ByName` or `With<Parameter>` (for example,
`CancelWorkRequirementByIdWithAbort`).

### 3. Update listeners can stop without telling you

Listeners such as `AddComputeRequirementListener` receive updates over a stream. If the stream fails, for example
because the connection drops, the listener stops receiving updates without reporting an error.

### 4. The module path will change

The SDK's module path, `github.com/yellowdog/yellowdog-sdk-go`, is temporary. When the SDK is published, it will
move to a new path, and imports of the SDK will need to change.

package main

import (
	"context"

	"github.com/kormiltsev/pgreader/methods"
)

func main() {
	ctx := context.Background()
	methods.PrintPostgressTablesInfo(ctx)
}

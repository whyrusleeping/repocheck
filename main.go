package main

import (
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/ipfs/go-ipfs/blocks/blockstore"
	"github.com/ipfs/go-ipfs/blocks/key"
	"github.com/ipfs/go-ipfs/merkledag"
	"github.com/ipfs/go-ipfs/repo/fsrepo"
	"github.com/ipfs/go-ipfs/unixfs"

	"golang.org/x/net/context"
)

type Dirent struct {
	Name  string
	Hash  key.Key
	Count int
}

type DirList []Dirent

func (dl DirList) Len() int           { return len(dl) }
func (dl DirList) Swap(i, j int)      { dl[i], dl[j] = dl[j], dl[i] }
func (dl DirList) Less(i, j int) bool { return dl[i].Count > dl[j].Count }

func main() {
	fmt.Printf("opening repo at %s\n", os.Args[1])
	r, err := fsrepo.Open(os.Args[1])
	if err != nil {
		panic(err)
	}

	fmt.Println("repo opened, starting key channel")
	bs := blockstore.NewBlockstore(r.Datastore())
	kc, err := bs.AllKeysChan(context.Background())
	if err != nil {
		panic(err)
	}

	fmt.Println("key channel started. beginning processing")

	t := make(map[string]int)
	errtypes := make(map[string]int)
	dirents := make(map[string]*Dirent)
	var count int
	for k := range kc {
		count++
		if count%1000 == 0 {
			fmt.Printf("processed: %d\n", count)
		}
		blk, err := bs.Get(k)
		if err != nil {
			panic(err)
		}

		nd, err := merkledag.Decoded(blk.Data)
		if err != nil {
			errtypes["notdag"]++
			continue
		}

		upb, err := unixfs.FromBytes(nd.Data)
		if err != nil {
			switch {
			case strings.Contains(err.Error(), "proto:"):
				errtypes["not unixfs"]++
			default:
				errtypes[err.Error()]++
			}
			continue
		}

		t[upb.GetType().String()]++

		if upb.GetType() == unixfs.TDirectory {
			for _, l := range nd.Links {
				tag := l.Name + string(l.Hash)
				e, ok := dirents[tag]
				if !ok {
					e = &Dirent{
						Name: l.Name,
						Hash: key.Key(l.Hash),
					}
					dirents[tag] = e
				}
				e.Count++
			}
		}
	}

	var dl DirList
	for _, d := range dirents {
		dl = append(dl, *d)
	}
	sort.Sort(dl)

	for i := 0; i < 10; i++ {
		fmt.Printf("%s %s %d\n", dl[i].Name, dl[i].Hash.B58String(), dl[i].Count)
	}
	fmt.Println(t)
	fmt.Println(errtypes)
}

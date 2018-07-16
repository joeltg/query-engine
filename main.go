package main

import (
	"fmt"
	"log"
	"reflect"
	"strings"
	"time"

	"encoding/json"

	cayley "github.com/cayleygraph/cayley"
	quad "github.com/cayleygraph/cayley/quad"
	ipfs "github.com/ipfs/go-ipfs-api"
	ld "github.com/piprate/json-gold/ld"
)

func ingestDocument(label string, doc interface{}, store *cayley.Handle) {
	processor := ld.NewJsonLdProcessor()
	options := ld.NewJsonLdOptions("")
	api := ld.NewJsonLdApi()

	expanded, err := processor.Expand(doc, options)
	if err != nil {
		log.Fatalln("processor error:", err)
		return
	}
	dataset, err := api.ToRDF(expanded, options)
	transaction := cayley.NewTransaction()
	for key, quads := range dataset.Graphs {
		for _, v := range quads {
			subject := v.Subject.GetValue()
			predicate := v.Predicate.GetValue()
			object := v.Object.GetValue()
			graph := label
			if v.Graph != nil {
				graph += "/" + v.Graph.GetValue()
			}
			q := quad.Make(subject, predicate, object, graph)
			fmt.Println(key, q)
			transaction.AddQuad(q)
		}
	}
	if err := store.ApplyTransaction(transaction); err != nil {
		log.Fatalln("error transaction", err)
	}
}

func handleAssertion(shell *ipfs.Shell, msg ipfs.PubSubRecord, store *cayley.Handle) {
	s := string(msg.Data())
	var doc interface{}
	err := shell.DagGet(s, &doc)
	if err != nil {
		log.Fatalln("dag error:", err)
		return
	}
	ingestDocument(s, doc, store)
}

func awaitAssertion(shell *ipfs.Shell, subscription *ipfs.PubSubSubscription, store *cayley.Handle) {
	for {
		msg, err := subscription.Next()
		if err == nil {
			handleAssertion(shell, msg, store)
		} else {
			log.Fatalln("assertion sub error:", err)
		}
	}
}

type path = []string

// Filter the light through your fingers
type Filter struct {
	Path  path
	Value string
}

// The Query is an curious animal
// For now, a query is a context iri, a {filter: value, path/to/prop: value2}, and a [properties, to/include, to/in/result]]
// The Path array can be empty to return everything the filter map matches
// The Filter map can be empty to return everything the path array resolves
type Query struct {
	Filter []Filter
	Values []path
}

// QueryResponse is a thing
type QueryResponse struct {
	Path   path
	Value  string
	Source string
}

func resolveQuery(query Query, store *cayley.Handle) []QueryResponse {
	path := cayley.StartPath(store).Tag("id").LabelContextWithTags([]string{"source"}, nil)

	for _, v := range query.Filter {
		if len(v.Path) > 1 {
			mp := cayley.StartMorphism()
			for len(v.Path) > 1 {
				mp = mp.Out(quad.String(v.Path[0]))
				v.Path = v.Path[1:]
			}
			path = path.Follow(mp)
		}

		path = path.Has(v.Path[0], quad.String(v.Value)).Back("id")
	}

	for i, v := range query.Values {
		if len(v) > 1 {
			mp := cayley.StartMorphism()
			for len(v) > 1 {
				mp = mp.Out(quad.String(v[0]))
				v = v[1:]
			}
			path = path.Follow(mp)
		}

		p := path.Out(quad.String(v[0]), v[0]).Tag(strings.Join(v, " "))
		if i > 0 {
			path = path.Or(p)
		} else {
			path = p
		}
	}

	responses := []QueryResponse{}
	err := path.Unique().Iterate(nil).TagValues(store, func(values map[string]quad.Value) {
		source, _ := values["source"]
		response := QueryResponse{Source: source.String()}
		for k, v := range values {
			if k != "id" && k != "source" {
				p := strings.Split(k, " ")
				response.Path = p
				response.Value = v.String()
			}
		}
		responses = append(responses, response)
	})
	if err != nil {
		log.Fatalln("tag error", err)
	}
	return responses
}

func handleQuery(shell *ipfs.Shell, msg ipfs.PubSubRecord, store *cayley.Handle) {
	s := string(msg.Data())
	fmt.Println("query:", s)

	var doc map[string]interface{}
	json.Unmarshal([]byte(s), &doc)

	context, hasContext := doc["@context"]

	processor := ld.NewJsonLdProcessor()
	options := ld.NewJsonLdOptions("")

	expanded, err := processor.Expand(doc, options)
	if err != nil {
		log.Fatalln("processor error:", err)
	}

	var query Query
	node := expanded[0].(map[string]interface{})
	fmt.Println("node", node)
	fmt.Println("type", reflect.TypeOf(node))
	rawFilter, hasFilter := node["http://underlay.mit.edu/query/filter"]
	rawValues, hasValues := node["http://underlay.mit.edu/query/values"]
	if !hasFilter || !hasValues {
		log.Fatalln("Invalid JSON-LD Query 1")
	}
	filter := rawFilter.([]interface{})
	values := rawValues.([]interface{})
	fs := make([]Filter, len(filter))
	for i, rawF := range filter {
		f := rawF.(map[string]interface{})
		rawPath, hasPath := f["http://underlay.mit.edu/query/path"]
		rawValue, hasValue := f["http://underlay.mit.edu/query/value"]
		path := rawPath.([]interface{})
		value := rawValue.([]interface{})
		if !hasPath || !hasValue || len(value) != 1 {
			log.Fatalln("Incalid JSON-LD Query 2")
		}
		p := make([]string, len(path))
		hasI := true
		for j, rawE := range path {
			e := rawE.(map[string]interface{})
			se, hasJ := e["@id"]
			p[j] = se.(string)
			hasI = hasI && hasJ
		}
		rawVW := value[0]
		vw := rawVW.(map[string]interface{})
		v, hasV := vw["@value"]
		if hasV && hasI {
			sv := v.(string)
			fs[i] = Filter{Path: p, Value: sv}
		} else {
			log.Fatalln("Invalid JSON-LD Query 3")
		}
	}
	vs := make([][]string, len(values))
	for i, rawV := range values {
		v := rawV.(map[string]interface{})
		rawPath, hasPath := v["http://underlay.mit.edu/query/path"]
		if !hasPath {
			log.Fatalln("Invalid JSON-LD Query 4")
		}
		path := rawPath.([]interface{})
		p := make([]string, len(path))
		hasI := true
		for j, rawE := range path {
			e := rawE.(map[string]interface{})
			se, hasJ := e["@id"]
			p[j] = se.(string)
			hasI = hasI && hasJ
		}
		if hasI {
			vs[i] = p
		} else {
			log.Fatalln("Invalid JSON-LD Query 5")
		}
	}
	query = Query{Filter: fs, Values: vs}

	responses := resolveQuery(query, store)
	type entry = map[string][]map[string]string
	entries := make([]entry, len(responses))
	for i, response := range responses {
		p := make([]map[string]string, len(response.Path))
		for j, l := range response.Path {
			p[j] = map[string]string{"@id": l}
		}
		v := []map[string]string{map[string]string{"@value": response.Value}}
		entries[i] = entry{"http://underlay.mit.edu/query/path": p, "http://underlay.mit.edu/query/value": v}
	}
	var compact interface{}
	if hasContext {
		res := map[string]interface{}{"@context": context, "http://underlay.mit.edu/query/values": entries}
		compact, err = processor.Compact(res, context, options)
	} else {
		res := map[string]interface{}{"http://underlay.mit.edu/query/values": entries}
		compact = []interface{}{res}
	}
	bytes, err := json.Marshal(compact)
	if err != nil {
		log.Fatalln("Could not serialize", err)
	}

	f := msg.From().Pretty()
	topic := "http://underlay.mit.edu/query/" + f
	err = shell.PubSubPublish(topic, string(bytes))
	if err != nil {
		log.Fatalln("query pub error", err)
	}
}

func awaitQuery(shell *ipfs.Shell, subscription *ipfs.PubSubSubscription, store *cayley.Handle) {
	for {
		msg, err := subscription.Next()
		if err == nil {
			handleQuery(shell, msg, store)
		} else {
			log.Fatalln("query sub error:", err)
		}
	}
}

func main() {
	fmt.Println("Hello world!")

	// Create Cayley instance
	store, err := cayley.NewMemoryGraph()
	if err != nil {
		log.Fatalln(err)
	}

	// Attach to IPFS daemon
	shell := ipfs.NewShell("localhost:5001")
	id, err := shell.ID()
	if err != nil {
		log.Fatalln("shell error:", err)
	}

	fmt.Println("id:", id.ID)

	// Attach assertion listener
	assertionTopic := "http://underlay.mit.edu/assertion"
	assertionSubscription, err := shell.PubSubSubscribe(assertionTopic)
	if err != nil {
		log.Fatalln("assertion pubsub error:", err)
	} else {
		go awaitAssertion(shell, assertionSubscription, store)
	}

	// Attach query listener
	queryTopic := "http://underlay.mit.edu/query"
	querySubscription, err := shell.PubSubSubscribe(queryTopic)
	if err != nil {
		log.Fatalln("query pubsub error:", err)
	} else {
		go awaitQuery(shell, querySubscription, store)
	}

	// Attach query response listener
	queryResponseTopic := queryTopic + "/" + id.ID
	queryResponseSubscription, err := shell.PubSubSubscribe(queryResponseTopic)
	if err != nil {
		log.Fatalln("query response pubsub error:", err)
	} else {
		go awaitQueryResponse(queryResponseSubscription)
	}

	// Do the thing
	go test(shell, store)

	// Wait forever
	select {}
}

func awaitQueryResponse(subscription *ipfs.PubSubSubscription) {
	for {
		msg, err := subscription.Next()
		if err == nil {
			handleQueryResponse(msg)
		} else {
			log.Fatalln("query sub error:", err)
		}
	}
}

func handleQueryResponse(msg ipfs.PubSubRecord) {
	fmt.Println("Got response:", string(msg.Data()))
}

func test(shell *ipfs.Shell, store *cayley.Handle) {
	// doc := `{"@context":{"sec":"http://purl.org/security#","xsd":"http://www.w3.org/2001/XMLSchema#","rdf":"http://www.w3.org/1999/02/22-rdf-syntax-ns#","dc":"http://purl.org/dc/terms/","sec:signer":{"@type":"@id"},"dc:created":{"@type":"xsd:dateTime"}},"@id":"http://example.org/sig1","@type":["rdf:Graph","sec:SignedGraph"],"dc:created":"2011-09-23T20:21:34Z","sec:signer":"http://payswarm.example.com/i/john/keys/5","sec:signatureValue":"OGQzNGVkMzVm4NTIyZTkZDYMmMzQzNmExMgoYzI43Q3ODIyOWM32NjI=","@graph":{"@id":"http://example.org/fact1","dc:title":"Hello World!"}}`
	doc2 := `{"@context":{"name":"http://rdf.data-vocabulary.org/#name","ingredient":"http://rdf.data-vocabulary.org/#ingredients","yield":"http://rdf.data-vocabulary.org/#yield","instructions":"http://rdf.data-vocabulary.org/#instructions","step":{"@id":"http://rdf.data-vocabulary.org/#step","@type":"xsd:integer"},"description":"http://rdf.data-vocabulary.org/#description","xsd":"http://www.w3.org/2001/XMLSchema#"},"name":"Mojito","ingredient":["12 fresh mint leaves","1/2 lime, juiced with pulp","1 tablespoons white sugar","1 cup ice cubes","2 fluid ounces white rum","1/2 cup club soda"],"yield":"1 cocktail","instructions":[{"step":1,"description":"Crush lime juice, mint and sugar together in glass."},{"step":2,"description":"Fill glass to top with ice cubes."},{"step":3,"description":"Pour white rum over ice."},{"step":4,"description":"Fill the rest of glass with club soda, stir."},{"step":5,"description":"Garnish with a lime wedge."}]}`
	c, err := shell.DagPut(doc2, "json", "cbor")
	if err != nil {
		log.Fatalln("error:", err)
		return
	}
	fmt.Println("c:", c)

	assertionTopic := "http://underlay.mit.edu/assertion"
	err = shell.PubSubPublish(assertionTopic, c)
	if err != nil {
		log.Fatalln("publish assertion error:", err)
	}
	time.Sleep(2 * time.Second)
	fmt.Println("wow look at this")
	// q := `{"context":null,"filter":[{"path":["http://rdf.data-vocabulary.org/#name"],"value":"Mojito"}],"path":[["http://rdf.data-vocabulary.org/#yield"]]}`
	q := `{"@context":{"rdf":"http://rdf.data-vocabulary.org/#","ul":"http://underlay.mit.edu/query/","ul:path":{"@type":"@id"}},"ul:filter":[{"ul:path":["rdf:name"],"ul:value":"Mojito"}],"ul:values":[{"ul:path":["rdf:yield"]}]}`
	err = shell.PubSubPublish("http://underlay.mit.edu/query", q)
	if err != nil {
		log.Fatalln("publish query error:", err)
	}
}

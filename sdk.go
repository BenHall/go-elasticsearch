package elasticsearch

//TODO: Manage json serialisation better - dotted everywhere. Needs a central method

import (
	"bytes"
	"fmt"
	"encoding/json"
	"net/http"
	"io/ioutil"
	"os"
)

type ElasticInsertResponse struct {
	Ok    bool `json:"ok"`
	Index 	string `json:"_index"`
	Type 	string `json:"_type"`
	Id 	string `json:"_id"`
	Version 	int `json:"_version"`
}

type ElasticGetResponse struct {
	Found    bool `json:"found"`
	Source 	*json.RawMessage `json:"_source"`

	Index 	string `json:"_index"`
	Type 	string `json:"_type"`
	Id 		string `json:"_id"`
	Version 	int `json:"_version"`
}

type SearchResult struct {
	RawJSON      []byte
	Took         int             `json:"took"`
	TimedOut     bool            `json:"timed_out"`
	//ShardStatus  Status          `json:"_shards"`
	Hits         Hits            `json:"hits"`
	Facets       json.RawMessage `json:"facets,omitempty"` // structure varies on query
	ScrollId     string          `json:"_scroll_id,omitempty"`
	Aggregations json.RawMessage `json:"aggregations,omitempty"` // structure varies on query
}

func (sr SearchResult) GetResults() []*json.RawMessage {
    var results []*json.RawMessage

    for _,element := range sr.Hits.Hits {
      results = append(results, element.Source)
    } 

    return results
}

type Hits struct {
	Total int `json:"total"`
	//	MaxScore float32 `json:"max_score"`
	Hits []Hit `json:"hits"`
}

func (h *Hits) Len() int {
	return len(h.Hits)
}

type Hit struct {
	Index       string           `json:"_index"`
	Type        string           `json:"_type,omitempty"`
	Id          string           `json:"_id"`
	//Score       Float32Nullable  `json:"_score,omitempty"` // Filters (no query) dont have score, so is null
	Source      *json.RawMessage `json:"_source"`          // marshalling left to consumer
	Fields      *json.RawMessage `json:"fields"`           // when a field arg is passed to ES, instead of _source it returns fields
	//Explanation *Explanation     `json:"_explanation,omitempty"`
}


func Save(index string, data interface{}) ElasticInsertResponse {
	baseUrl := baseUrl()
	url := fmt.Sprintf("%v/%v", baseUrl, index)

	return save(url, data);
}

func SaveWithId(index string, id string, data interface{}) ElasticInsertResponse {
	baseUrl := baseUrl()
	url := fmt.Sprintf("%v/%v/%v", baseUrl, index, id)

	return save(url, data);
}

func save(url string, data interface{}) ElasticInsertResponse {
	body, _ := json.MarshalIndent(&data, "", "  ")

	response, err := http.Post(url, "application/json", bytes.NewBuffer(body))
	if err != nil {
		fmt.Printf("Error: %s", err)
	}

	defer response.Body.Close()

	body, err2 := ioutil.ReadAll(response.Body)
	if err2 != nil {
		fmt.Printf("Error: %s", err2)
	}

	var insert_response ElasticInsertResponse
	json.Unmarshal(body, &insert_response)

    return insert_response;
}

func Get(index string, id string) ElasticGetResponse {
	baseUrl := baseUrl()
	url := fmt.Sprintf("%v/%v/%v", baseUrl, index, id)

	response, err := http.Get(url)
	if err != nil {
		fmt.Printf("Error: %s", err)
	}

	defer response.Body.Close()

	body, err2 := ioutil.ReadAll(response.Body)
	if err2 != nil {
		fmt.Printf("Error: %s", err2)
	}

	var parsed_resp ElasticGetResponse
	json.Unmarshal([]byte(body), &parsed_resp)

	return parsed_resp; //TODO: Return an errors
}

func SearchRange(index string, on string, before string, after string) SearchResult {
	baseUrl := baseUrl()
	url := fmt.Sprintf("%v/%v/_search", baseUrl, index)
	data := fmt.Sprintf("{\"size\" : 10000, \"query\" : { \"range\" : {\"%v\": { \"gte\": \"%v\", \"lte\": \"%v\"}}}}", on, before, after)

	response, err := http.Post(url, "application/json", bytes.NewBufferString(data))
	if err != nil {
		fmt.Printf("Error: %s", err)
	}
	defer response.Body.Close()

	body, err2 := ioutil.ReadAll(response.Body)
	if err2 != nil {
		fmt.Printf("Error: %s", err2)
	}

	var parsed_resp SearchResult
	json.Unmarshal([]byte(body), &parsed_resp)
	parsed_resp.RawJSON = body

	return parsed_resp;
}
func SearchIndex(index string, on string, term string) SearchResult {
	baseUrl := baseUrl()
	url := fmt.Sprintf("%v/%v/_search?size=10000&q=%v:%v", baseUrl, index, on, term)

	response, err := http.Get(url)
	if err != nil {
		fmt.Printf("Error: %s", err)
	}
		fmt.Printf("%s", url)

	defer response.Body.Close()

	body, err2 := ioutil.ReadAll(response.Body)
	if err2 != nil {
		fmt.Printf("Error: %s", err2)
	}

	var parsed_resp SearchResult
	json.Unmarshal([]byte(body), &parsed_resp)
	parsed_resp.RawJSON = body

	return parsed_resp;
}
func SearchIndexForSingleRecord(index string, on string, term string) SearchResult {
	baseUrl := baseUrl()
	url := fmt.Sprintf("%v/%v/_search?size=1&q=%v:%v", baseUrl, index, on, term)

	response, err := http.Get(url)
	if err != nil {
		fmt.Printf("Error: %s", err)
	}
		fmt.Printf("%s", url)

	defer response.Body.Close()

	body, err2 := ioutil.ReadAll(response.Body)
	if err2 != nil {
		fmt.Printf("Error: %s", err2)
	}

	var parsed_resp SearchResult
	json.Unmarshal([]byte(body), &parsed_resp)
	parsed_resp.RawJSON = body

	return parsed_resp;
}
func Search(on string, term string) SearchResult {
	baseUrl := baseUrl()
	url := fmt.Sprintf("%v/_search?size=10000&q=%v:%v", baseUrl, on, term)

	response, err := http.Get(url)
	if err != nil {
		fmt.Printf("Error: %s", err)
	}
	defer response.Body.Close()

	body, err2 := ioutil.ReadAll(response.Body)
	if err2 != nil {
		fmt.Printf("Error: %s", err2)
	}

	var parsed_resp SearchResult
	json.Unmarshal([]byte(body), &parsed_resp)
	parsed_resp.RawJSON = body

	return parsed_resp;
}

func baseUrl() string {
	var url string

	url = os.Getenv("ELASTICSEARCH_URL")

	return url
}



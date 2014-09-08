package elasticsearch

import (
	"testing"
	"encoding/json"
	"github.com/stretchr/testify/assert"
)

type TestFixture struct {
	Id    string `json:"id"`
	Home  string `json:"home"`
	Away  string `json:"away"`
}

func TestSave_creates_fixture_in_search(t *testing.T) {
	index := "test_fixtures"

	test_data := []TestFixture{ 
		{ Id: "1.2", Home: "3", Away: "3" },
	  }

	resp := Save(index, test_data[0]);
	assert.Equal(t, resp.Ok, true)
}

func TestSaveWithId_creates_id(t *testing.T) {
	index := "test_fixtures"
	test_data := []TestFixture{ 
		{ Id: "1.2", Home: "3", Away: "3" },
	  }

	resp := SaveWithId(index, "12345", test_data[0]);
	assert.Equal(t, resp.Ok, true)

	var parsed_resp TestFixture
	fixture := Get(index, "12345")
	json.Unmarshal(*fixture.Source, &parsed_resp)

	assert.Equal(t, parsed_resp.Id, "1.2")
	assert.Equal(t, parsed_resp.Home, "3")
	assert.Equal(t, parsed_resp.Away, "3")
}


func TestGet_returns_data(t *testing.T) {
	index := "test_fixtures"
	test_data := []TestFixture{ 
		{ Id: "1.2", Home: "3", Away: "3" },
	  }

	resp := Save(index, test_data[0]);
	assert.Equal(t, resp.Ok, true)

	var parsed_resp TestFixture
	fixture := Get(index, resp.Id)
	json.Unmarshal(*fixture.Source, &parsed_resp)

	assert.Equal(t, parsed_resp.Id, "1.2")
	assert.Equal(t, parsed_resp.Home, "3")
	assert.Equal(t, parsed_resp.Away, "3")
}

func TestGet_not_found_result_is_nil(t *testing.T) {
	fixture := Get("test_fixtures", "Some Unknown Value")

	assert.Equal(t, fixture.Exists, false)
	assert.Equal(t, fixture.Source, (*json.RawMessage)(nil))
	assert.Equal(t, fixture.Source == nil, true)
}

/*
func TestSearchRange_returns_data(t *testing.T) {
	//bad test. Should setup data 
	fixtures := SearchRange("fixtures", "date", "20140816", "20140816");

	//http://stackoverflow.com/questions/23255456/whats-the-proper-way-to-convert-a-json-rawmessage-to-a-struct
	var parsed_resp TestFixture
	json.Unmarshal(*fixtures.Hits.Hits[0].Source, &parsed_resp) //Extend json.RawMessage ?

	assert.Equal(t, parsed_resp.Home, "Arsenal")
}


func TestSearch_returns_data(t *testing.T) {
	//bad test. Should setup data 
	fixtures := Search("user_id", "1");

	assert.NotEqual(t, *fixtures.Hits.Hits[0].Source, "")
}
*/

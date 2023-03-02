import argparse
import json
import urllib.request as req

def parse_endpoint_rule_set(input_bytes: bytes, output_path: str, pretty: bool):
    model = json.loads(input_bytes)
    
    endpoint_rule_set = model["shapes"]["com.amazonaws.s3#AmazonS3"]["traits"]["smithy.rules#endpointRuleSet"]

    with open(output_path, "w") as f:
        json.dump(endpoint_rule_set, f, indent=2 if pretty else None)

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("input_path", help="URL or path for the S3 smithy model")
    p.add_argument("output_path", help="Output path for the endpoints JSON blob")
    p.add_argument("--pretty-print", action="store_true", help="Pretty-print the resulting JSON")
    args = p.parse_args()

    if args.input_path.startswith("http"):
        with req.urlopen(args.input_path) as r:
            body = r.read()
    else:
        with open(args.input_path, "rb") as f:
            body = f.read()
    
    parse_endpoint_rule_set(body, args.output_path, args.pretty_print)

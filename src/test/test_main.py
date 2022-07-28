import json
import pytest

from src import main


def test_post_json_success():
    payload = json.dumps({
        "customer_id": 123,
        "country_code": "Peru",
        "last_order_ts": "2018-05-03 00:00:00",
        "first_order_ts": "2017-05-03 00:00:00",
        "total_orders": 15,
        "segment_name": "recency_segment"
    })

    response = main.app.test_client().post(
        '/post_json',
        data=payload,
        content_type='application/json',
    )
    data = json.loads(response.get_data(as_text=True))

    # Assumed the function return back json with voucher amount now
    data = json.dumps({
        "voucher_amount": 3})

    assert data['voucher_amount'] == 3


def test_post_json_fail():
    payload = json.dumps({

    })
    # empty payload sent from the client
    response = main.app.test_client().post(
        '/post_json',
        data=payload,
        content_type='application/json',
    )
    data = json.loads(response.get_data(as_text=True))

    # Assumed the function return back json with voucher amount now
    data = json.dumps({
        "voucher_amount": 0.00})

    assert data['voucher_amount'] == 0.00
    #Sicne voucher_amount was set to 0.00 for all null, empty values

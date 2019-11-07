const mongoose = require('mongoose')
const Float = require('mongoose-float').loadType(mongoose)
const fs = require('fs')
const csv = require('fast-csv')
const chokidar = require('chokidar');
const express = require('express')
const app = express()

mongoose.connect('mongodb://localhost:27017/banco_dev', {
    useNewUrlParser: true,
    useUnifiedTopology: true
})

const sellModel = mongoose.model('sell', new mongoose.Schema({
    num_ped: String,
    seq_ped: String,
    client: String,
    seller: String,
    status: String,
    dt_sell: Date,
    cat_prod: String,
    fam_prod: String,
    cod_prod: String,
    sell_value: Float,
    sell_quant: Float,
    oc: String
}))

const metaModel = mongoose.model('meta', new mongoose.Schema({
    ano:String,
    mes:String,
    vendedor:String,
    produto:String,
    qunatidade:Number
}))

const watcher = chokidar.watch('data', {ignored: /^\./, persistent: true});

const end_timeout = 30000;

watcher
    .on('add', function(path) {

        console.log('File', path, 'has been added');

        fs.stat(path, function (err, stat) {
            // Replace error checking with something appropriate for your app.
            if (err) throw err;
            setTimeout(checkEnd, end_timeout, path, stat);
        });
});

function checkEnd(path, prev) {
    fs.stat(path, function (err, stat) {

        // Replace error checking with something appropriate for your app.
        if (err) throw err;
        if (stat.mtime.getTime() === prev.mtime.getTime()) {

        }
        else
            setTimeout(checkEnd, end_timeout, path, stat);
    });
}
const calculateSellQuantity = (qntCX, qtdUND, qntUN, retCX, retUN) => {
    const total_qnt = (parseInt(qntCX) * parseInt(qtdUND) + parseInt(qntUN)) / parseInt(qtdUND)
    const total_ret = (parseInt(retCX) * parseInt(qtdUND) + parseInt(retUN)) / parseInt(qtdUND)
    return total_qnt - total_ret
}

const calculateSellValue = (totalValue, returnValue) => {
    const total_value = parseFloat(totalValue).toFixed(2)
    const return_value = parseFloat(returnValue).toFixed(2)
    return total_value - return_value
}

const toDate = (dateStr) => {
    const [day, month, year] = dateStr.split("/")
    return new Date(year, month - 1, day)
}

const removeDuplcates = (arr) => arr.filter((este, i) => arr.indexOf(este) === i)

const checkVolume_curinga02 = (sell) => {
    let isValid = false
    const fam = ['0000902411', '0000902852', '0000902410', '0000902406', '0000902451']
    fam.map(element => { if (element === sell.cod_prod) { isValid = true } })
    return isValid
}

const checkVolume_curinga01 = (sell) => {
    let isValid = false
    const fam = ['0000903061', '0000903126', '0000901161', '0000903131', '0000903129', '0000903130']
    fam.map(element => { if (element === sell.cod_prod) { isValid = true } })
    return isValid
}

const checkVolume_premium = (sell) => {
    let isValid = false
    const fam = ['010', '081', '040', '004']
    fam.map(element => {
        if (element === sell.fam_prod || '0000903750' === sell.cod_prod) {
            isValid = true
        }
        if ('0000901161' === sell.cod_prod) {
            isValid = false
        }
    })
    return isValid
}

const checkVolume_600_300_1lt = (sell) => {
    let isValid = false
    const fam = ['001', '004', '025', '061', '062', '081']
    fam.map(element => { if (element === sell.fam_prod) { isValid = true } })
    return isValid
}

const checkCoberturaDevassa600 = (sell) => {
    let isValid = false
    const fam = ['0000903061']
    fam.map(element => { if (element === sell.cod_prod) { isValid = true } })
    return isValid
}

const checkCoberturaDevassaTotal = (sell) => {
    let isValid = false
    const fam = ['0000903061', '0000901161', '0000903062', '0000903130', '0000903129']
    fam.map(element => { if (element === sell.cod_prod) { isValid = true } })
    return isValid
}

const checkCoberturaEisenbahn600 = (sell) => {
    let isValid = false
    const fam = ['0000903024', '0000903025']
    fam.map(element => { if (element === sell.cod_prod) { isValid = true } })
    return isValid
}

const checkCoberturaEisenbahnTotal = (sell) => {
    let isValid = false
    const fam = ['0000903024', '0000901781', '0000903212', '0000903561', '0000900820', '0000902241', '0000902544', '0000903025']
    fam.map(element => { if (element === sell.cod_prod) { isValid = true } })
    return isValid
}

const checkCoberturaSchin600 = (sell) => {
    let isValid = false
    const fam = ['0000902411']
    fam.map(element => { if (element === sell.cod_prod) { isValid = true } })
    return isValid
}

const checkCoberturaSchinTotal = (sell) => {
    let isValid = false
    const fam = ['0000902411', '0000902406', '0000902410', '0000902432', '0000902452', '0000902451']
    fam.map(element => { if (element === sell.cod_prod) { isValid = true } })
    return isValid
}

const checkCoberturaNA = (sell) => {
    let isValid = false
    const fam = ['200', '210', '245', '255', '310', '360', '437', '033']
    fam.map(element => { if (element === sell.fam_prod) { isValid = true } })
    return isValid
}

const checkCoberturaCervejas600 = (sell) => {
    let isValid = false
    const fam = ['001', '004', '081']
    fam.map(element => { if (element === sell.fam_prod) { isValid = true } })
    return isValid
}

const checkCoberturaCervejasTotal = (sell) => {
    let isValid = false
    const fam = ['001', '003', '004', '010', '025', '030', '040', '060', '061', '081']
    fam.map(element => { if (element === sell.fam_prod) { isValid = true } })
    return isValid
}

const checkIfOcurrency = (sell) => (sell['Ocorr'] === '001' || sell['Ocorr'] === '002' || sell['Ocorr'] === '004') ? true : false
const chechIfCancelled = (sell) => (sell['Canc'] === '000' || sell['Canc'] === '008') ? true : false
const chechIfCategory = (sell) => (sell['Cat Prod'] != '99') ? true : false
const checkSell = (sell) => (checkIfOcurrency(sell) && chechIfCancelled(sell) && chechIfCategory(sell)) ? true : false

importarMetas()
async function importarMetas() {
    try {
        fs.createReadStream('data/METAS.csv')
            .pipe(csv.parse({delimiter: ';', headers: false}))
            .on('data', async (row) => {
                try {
                    const meta = {
                        ano:row[0],
                        mes:row[1],
                        seller:row[2],
                        cod_prod:row[3].padStart(10,"0"),
                        fam_prod:row[4].padStart(3,"0"),
                        amount:parseInt(row[5])
                    }
                    const query = {ano:meta.ano , mes:meta.mes, vendedor:meta.seller, produto:meta.cod_prod}
                    await metaModel.findOneAndUpdate(query, meta, { upsert: true, new: true }).exec()
                } catch(error) {
                    console.log('error: ',error)
                }
            })
    } catch(error) {
        console.log('error: ',error)
    }
}

async function run() {
    try {
        fs.createReadStream('data/PEDIDOS.CSV')
            .pipe(csv.parse({ delimiter: ';', headers: true }))
            .on('data', async (row) => {
                try {
                    if (checkSell(row)) {
                        const sell = {
                            num_ped: row['Pedido'],
                            seq_ped: row['||Item'].replace('||', ''),
                            client: row['Cliente'],
                            seller: row['Vendedor'],
                            status: row['Canc'],
                            dt_sell: toDate(row['Dt.Pedido']),
                            cat_prod: row['Cat Prod'],
                            fam_prod: row['Fam'],
                            cod_prod: row['Cod Red'],
                            sell_value: calculateSellValue(row['Vlr Total'].replace(/,/g, '.'), row['Vlr Retorno'].replace(/,/g, '.')),
                            sell_quant: calculateSellQuantity(row['Quant CX'], row['QTD/UND'], row['Quant UN'], row['Retorno CX'], row['Retorno UN']),
                            oc: row['Ocorr']
                        }
                        const query = { num_ped: sell.num_ped, seq_ped: sell.seq_ped }
                        await sellModel.findOneAndUpdate(query, sell, { upsert: true, new: true }).exec()
                    }
                } catch (error) {
                    console.log('error: ', error)
                }
            })
            .on('end', () => console.log('importação finalizada com sucesso') )
    } catch (error) {
        console.log('deu error: ', error)
    }
}

app.get('/get-data-by-seller/:vd', async (req, res) => {
    let faturamento = 0
    let volume_300_600_1lt = 0
    let volume_premium = 0
    let volume_volume_total = 0
    let volume_curinga01 = 0
    let volume_curinga02 = 0
    let clientesCobTotal = []
    let clientesCobCervejasTotal = []
    let clientesCobCervejas600 = []
    let clientesCobNA = []
    let clientesCobSchin600 = []
    let clientesCobSchinTotal = []
    let clientesCobEisenbahn600 = []
    let clientesCobEisenbahnTotal = []
    let clientesCobDevassa600 = []
    let clientesCobDevassaTotal = []
    let dados = {}

    try {

        if (req.params.vd) {
            const pedidos = await sellModel.find({ seller: req.params.vd }).exec()
            pedidos.map(pedido => {
                faturamento += pedido.sell_value
                if (pedido.sell_value > 0) clientesCobTotal.push(pedido.client)
                if (pedido.sell_value > 0 && checkCoberturaCervejasTotal(pedido)) clientesCobCervejasTotal.push(pedido.client)
                if (pedido.sell_value > 0 && checkCoberturaCervejas600(pedido)) clientesCobCervejas600.push(pedido.client)
                if (pedido.sell_value > 0 && checkCoberturaNA(pedido)) clientesCobNA.push(pedido.client)
                if (pedido.sell_value > 0 && checkCoberturaSchin600(pedido)) clientesCobSchin600.push(pedido.client)
                if (pedido.sell_value > 0 && checkCoberturaSchinTotal(pedido)) clientesCobSchinTotal.push(pedido.client)
                if (pedido.sell_value > 0 && checkCoberturaEisenbahn600(pedido)) clientesCobEisenbahn600.push(pedido.client)
                if (pedido.sell_value > 0 && checkCoberturaEisenbahnTotal(pedido)) clientesCobEisenbahnTotal.push(pedido.client)
                if (pedido.sell_value > 0 && checkCoberturaDevassa600(pedido)) clientesCobDevassa600.push(pedido.client)
                if (pedido.sell_value > 0 && checkCoberturaDevassaTotal(pedido)) clientesCobDevassaTotal.push(pedido.client)

                volume_volume_total += pedido.sell_quant
                if (checkVolume_600_300_1lt(pedido)) volume_300_600_1lt += pedido.sell_quant
                if (checkVolume_premium(pedido)) volume_premium += pedido.sell_quant
                if (checkVolume_curinga01(pedido)) volume_curinga01 += pedido.sell_quant
                if (checkVolume_curinga02(pedido)) volume_curinga02 += pedido.sell_quant
            })

            dados = {
                real: {
                    faturamento: parseFloat(faturamento).toFixed(2),
                    coberturaTotal: removeDuplcates(clientesCobTotal).length,
                    coberturaCervejasTotal: removeDuplcates(clientesCobCervejasTotal).length,
                    coberturaCervejas600: removeDuplcates(clientesCobCervejas600).length,
                    coberturaNA: removeDuplcates(clientesCobNA).length,
                    coberturaSchin600: removeDuplcates(clientesCobSchin600).length,
                    coberturaSchinTotal: removeDuplcates(clientesCobSchinTotal).length,
                    coberturaEisenbahn600: removeDuplcates(clientesCobEisenbahn600).length,
                    coberturaEisenbahnTotal: removeDuplcates(clientesCobEisenbahnTotal).length,
                    coberturaDevassa600: removeDuplcates(clientesCobDevassa600).length,
                    coberturaDevassaTotal: removeDuplcates(clientesCobDevassaTotal).length,
                    volume_300_600_1lt,
                    volume_premium,
                    volume_volume_total,
                    volume_curinga01,
                    volume_curinga02
                },
                meta: {
                    faturamento: 0,
                    coberturaTotal: 0,
                    coberturaCervejasTotal: 0,
                    coberturaCervejas600: 0,
                    coberturaNA: 0,
                    coberturaSchin600: 0,
                    coberturaSchinTotal: 0,
                    coberturaEisenbahn600: 0,
                    coberturaEisenbahnTotal: 0,
                    coberturaDevassa600: 0,
                    coberturaDevassaTotal: 0,
                    volumeTrividro: 0,
                    volumePremium: 0,
                    volumeTotal: 0,
                    volumeCuringa01: 0,
                    volumeCuringa02: 0
                }
            }

        }
        return res.json(dados)
    } catch (error) {
        console.log('error: ', error)
    }
})

app.listen(3000, function () {
    console.log('Example app listening on port 3000!')
})